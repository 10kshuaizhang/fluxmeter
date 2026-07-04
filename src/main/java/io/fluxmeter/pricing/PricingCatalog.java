package io.fluxmeter.pricing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.fluxmeter.model.TokenEvent;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * External pricing catalog loaded from JSON file or Redis snapshot.
 * Supports flat, volume (block), and graduated tier pricing.
 */
public final class PricingCatalog {

    public enum PricingMode {
        FLAT, VOLUME, GRADUATED;

        static PricingMode fromString(String raw, boolean hasTiers) {
            if (raw == null || raw.isBlank()) {
                return hasTiers ? VOLUME : FLAT;
            }
            return switch (raw.toLowerCase()) {
                case "flat" -> FLAT;
                case "volume" -> VOLUME;
                case "graduated" -> GRADUATED;
                default -> throw new IllegalArgumentException("Unknown pricing_mode: " + raw);
            };
        }
    }

    /** Which Redis/Flink key scopes monthly volume (v2.4: customer_model only). */
    public enum VolumeScope {
        CUSTOMER_MODEL("customer_model");

        private final String jsonValue;

        VolumeScope(String jsonValue) {
            this.jsonValue = jsonValue;
        }

        static VolumeScope fromString(String raw) {
            if (raw == null || raw.isBlank()) {
                return CUSTOMER_MODEL;
            }
            for (VolumeScope scope : values()) {
                if (scope.jsonValue.equals(raw)) {
                    return scope;
                }
            }
            throw new IllegalArgumentException("Unknown volume_scope: " + raw);
        }

        public String jsonValue() {
            return jsonValue;
        }
    }

    public enum BillingPeriod {
        CALENDAR_MONTH("calendar_month");

        private final String jsonValue;

        BillingPeriod(String jsonValue) {
            this.jsonValue = jsonValue;
        }

        static BillingPeriod fromString(String raw) {
            if (raw == null || raw.isBlank()) {
                return CALENDAR_MONTH;
            }
            for (BillingPeriod period : values()) {
                if (period.jsonValue.equals(raw)) {
                    return period;
                }
            }
            throw new IllegalArgumentException("Unknown billing_period: " + raw);
        }

        public String jsonValue() {
            return jsonValue;
        }
    }

    private static volatile PricingCatalog INSTANCE = PricingCatalog.loadDefault();

    private final Map<String, ModelPricing> models;
    private final ModelPricing defaults;
    private final List<String> prefixModels;
    private final double cacheReadMultiplier;
    private final String version;
    private final VolumeScope volumeScope;
    private final BillingPeriod billingPeriod;

    private PricingCatalog(
            Map<String, ModelPricing> models,
            ModelPricing defaults,
            List<String> prefixModels,
            double cacheReadMultiplier,
            String version,
            VolumeScope volumeScope,
            BillingPeriod billingPeriod) {
        this.models = models;
        this.defaults = defaults;
        this.prefixModels = prefixModels;
        this.cacheReadMultiplier = cacheReadMultiplier;
        this.version = version;
        this.volumeScope = volumeScope;
        this.billingPeriod = billingPeriod;
    }

    public static PricingCatalog get() {
        return INSTANCE;
    }

    public static void reload(PricingCatalog catalog) {
        INSTANCE = catalog;
    }

    public String getVersion() {
        return version;
    }

    public VolumeScope getVolumeScope() {
        return volumeScope;
    }

    public BillingPeriod getBillingPeriod() {
        return billingPeriod;
    }

    public static PricingCatalog loadDefault() {
        String path = System.getenv().getOrDefault("PRICING_FILE", "config/pricing.json");
        try {
            File file = new File(path);
            if (file.exists()) {
                return loadFromBytes(Files.readAllBytes(file.toPath()));
            }
            InputStream in = PricingCatalog.class.getResourceAsStream("/pricing.json");
            if (in != null) {
                return loadFromBytes(in.readAllBytes());
            }
        } catch (Exception e) {
            System.err.println("PricingCatalog: failed to load " + path + ", using built-in fallback: " + e.getMessage());
        }
        return builtInFallback();
    }

    public static PricingCatalog loadFromBytes(byte[] json) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(json);
        return parse(root);
    }

    public static PricingCatalog parse(JsonNode root) {
        Map<String, ModelPricing> models = new HashMap<>();
        JsonNode modelsNode = root.get("models");
        if (modelsNode != null) {
            modelsNode.fields().forEachRemaining(entry -> {
                models.put(entry.getKey(), ModelPricing.fromJson(entry.getValue()));
            });
        }

        ModelPricing defaults = ModelPricing.fromJson(root.get("defaults"));
        if (defaults == null) {
            defaults = new ModelPricing(PricingMode.FLAT, 1.0, 3.0, 0.1, null);
        }

        List<String> prefixModels = new ArrayList<>();
        JsonNode prefixNode = root.get("prefix_models");
        if (prefixNode != null && prefixNode.isArray()) {
            prefixNode.forEach(n -> prefixModels.add(n.asText()));
        }

        double cacheMult = root.has("cache_read_multiplier")
                ? root.get("cache_read_multiplier").asDouble(0.5)
                : 0.5;

        String version = root.has("version") ? root.get("version").asText("1") : "1";
        VolumeScope volumeScope = VolumeScope.fromString(
                root.has("volume_scope") ? root.get("volume_scope").asText() : null);
        BillingPeriod billingPeriod = BillingPeriod.fromString(
                root.has("billing_period") ? root.get("billing_period").asText() : null);

        return new PricingCatalog(
                models, defaults, prefixModels, cacheMult, version, volumeScope, billingPeriod);
    }

    public String normalizeModelId(String model) {
        if (model == null || model.isEmpty()) {
            return "unknown";
        }
        if (models.containsKey(model)) {
            return model;
        }
        for (String prefix : prefixModels) {
            if (model.startsWith(prefix)) {
                return prefix;
            }
        }
        return model;
    }

    public long calculateEventCostMicro(TokenEvent event) {
        return calculateEventCostMicro(event, 0L);
    }

    /** Cost in microdollars; monthlyTokensBefore drives volume/graduated tier selection. */
    public long calculateEventCostMicro(TokenEvent event, long monthlyTokensBefore) {
        String model = normalizeModelId(event.getModelId());
        ModelPricing pricing = models.getOrDefault(model, defaults);
        return pricing.calculateEventCostMicro(event, monthlyTokensBefore, cacheReadMultiplier);
    }

    public double calculateEventCost(TokenEvent event) {
        return calculateEventCostMicro(event) / 1_000_000.0;
    }

    public String toJsonSnapshot() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> out = new HashMap<>();
        out.put("version", version);
        out.put("cache_read_multiplier", cacheReadMultiplier);
        out.put("volume_scope", volumeScope.jsonValue());
        out.put("billing_period", billingPeriod.jsonValue());
        Map<String, Object> modelMap = new HashMap<>();
        for (Map.Entry<String, ModelPricing> e : models.entrySet()) {
            modelMap.put(e.getKey(), e.getValue().toMap());
        }
        out.put("models", modelMap);
        out.put("defaults", defaults.toMap());
        out.put("prefix_models", prefixModels);
        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(out);
    }

    private static PricingCatalog builtInFallback() {
        Map<String, ModelPricing> models = new HashMap<>();
        models.put("gpt-4o", ModelPricing.flat(2.50, 10.00, 0.10));
        models.put("gpt-4o-mini", ModelPricing.flat(0.15, 0.60, 0.10));
        models.put("o1", ModelPricing.flat(15.00, 60.00, 0.10));
        models.put("o3-mini", ModelPricing.flat(1.10, 4.40, 0.10));
        models.put("claude-opus-4", ModelPricing.flat(15.00, 75.00, 0.10));
        models.put("claude-sonnet-4", ModelPricing.flat(3.00, 15.00, 0.10));
        models.put("claude-haiku-4", ModelPricing.flat(0.80, 4.00, 0.10));
        models.put("gemini-1.5-pro", ModelPricing.flat(3.50, 10.50, 0.10));
        models.put("gemini-1.5-flash", ModelPricing.flat(0.075, 0.30, 0.10));
        models.put("text-embedding-3-small", ModelPricing.flat(0.02, 3.0, 0.02));
        models.put("text-embedding-3-large", ModelPricing.flat(0.13, 3.0, 0.13));
        models.put("deepseek-v4-flash", ModelPricing.flat(0.14, 0.28, 0.10));
        models.put("deepseek-v4-pro", ModelPricing.flat(0.435, 0.87, 0.10));
        models.put("deepseek-chat", ModelPricing.flat(0.27, 1.10, 0.10));
        models.put("deepseek-reasoner", ModelPricing.flat(0.55, 2.19, 0.10));
        models.put("qwen-max", ModelPricing.flat(1.60, 6.40, 0.10));
        models.put("qwen-plus", ModelPricing.flat(0.40, 1.20, 0.10));
        models.put("qwen-turbo", ModelPricing.flat(0.10, 0.30, 0.10));
        models.put("qwen-long", ModelPricing.flat(0.50, 2.00, 0.10));
        models.put("glm-4", ModelPricing.flat(0.50, 0.50, 0.10));
        models.put("glm-4-flash", ModelPricing.flat(0.06, 0.06, 0.10));
        models.put("glm-4-air", ModelPricing.flat(0.10, 0.10, 0.10));
        models.put("moonshot-v1-8k", ModelPricing.flat(0.20, 2.00, 0.10));
        models.put("moonshot-v1-32k", ModelPricing.flat(1.00, 3.00, 0.10));
        models.put("moonshot-v1-128k", ModelPricing.flat(2.00, 5.00, 0.10));
        models.put("doubao-pro-32k", ModelPricing.flat(0.80, 2.00, 0.10));
        models.put("doubao-lite-32k", ModelPricing.flat(0.30, 0.60, 0.10));
        models.put("baichuan4-turbo", ModelPricing.flat(0.50, 0.50, 0.10));
        models.put("abab6.5-chat", ModelPricing.flat(0.30, 1.00, 0.10));
        models.put("hunyuan-lite", ModelPricing.flat(0.00, 0.00, 0.10));
        models.put("hunyuan-pro", ModelPricing.flat(0.40, 1.20, 0.10));
        List<String> prefixes = List.of(
                "gpt-4o-mini", "gpt-4o", "o3-mini", "o1",
                "claude-opus-4", "claude-sonnet-4", "claude-haiku-4",
                "gemini-1.5-pro", "gemini-1.5-flash",
                "text-embedding-3-large", "text-embedding-3-small",
                "moonshot-v1-128k", "moonshot-v1-32k", "moonshot-v1-8k",
                "deepseek-v4-pro", "deepseek-v4-flash", "deepseek-reasoner", "deepseek-chat",
                "qwen-max", "qwen-plus", "qwen-turbo", "qwen-long",
                "glm-4-flash", "glm-4-air", "glm-4",
                "doubao-pro-32k", "doubao-lite-32k",
                "baichuan4-turbo", "abab6.5-chat",
                "hunyuan-lite", "hunyuan-pro");
        return new PricingCatalog(
                models,
                new ModelPricing(PricingMode.FLAT, 1.0, 3.0, 0.1, null),
                prefixes,
                0.5,
                "builtin",
                VolumeScope.CUSTOMER_MODEL,
                BillingPeriod.CALENDAR_MONTH);
    }

    public static final class ModelPricing {
        private final PricingMode pricingMode;
        private final double inputPerM;
        private final double outputPerM;
        private final double embeddingPerM;
        private final List<Tier> tiers;

        public ModelPricing(
                PricingMode pricingMode,
                double inputPerM,
                double outputPerM,
                double embeddingPerM,
                List<Tier> tiers) {
            this.pricingMode = pricingMode;
            this.inputPerM = inputPerM;
            this.outputPerM = outputPerM;
            this.embeddingPerM = embeddingPerM;
            this.tiers = tiers != null ? tiers : Collections.emptyList();
        }

        static ModelPricing flat(double input, double output, double embedding) {
            return new ModelPricing(PricingMode.FLAT, input, output, embedding, null);
        }

        static ModelPricing fromJson(JsonNode node) {
            if (node == null || node.isNull()) {
                return null;
            }
            double input = node.has("input_per_m") ? node.get("input_per_m").asDouble() : 1.0;
            double output = node.has("output_per_m") ? node.get("output_per_m").asDouble() : 3.0;
            double embedding = node.has("embedding_per_m") ? node.get("embedding_per_m").asDouble() : 0.10;

            List<Tier> tiers = new ArrayList<>();
            JsonNode tiersNode = node.get("tiers");
            if (tiersNode != null && tiersNode.isArray()) {
                for (JsonNode t : tiersNode) {
                    Long upTo = t.has("up_to_tokens_m") && !t.get("up_to_tokens_m").isNull()
                            ? t.get("up_to_tokens_m").asLong()
                            : null;
                    tiers.add(new Tier(
                            upTo,
                            t.has("input_per_m") ? t.get("input_per_m").asDouble() : input,
                            t.has("output_per_m") ? t.get("output_per_m").asDouble() : output,
                            t.has("embedding_per_m") ? t.get("embedding_per_m").asDouble() : embedding));
                }
            }

            boolean hasTiers = !tiers.isEmpty();
            String modeRaw = node.has("pricing_mode") ? node.get("pricing_mode").asText() : null;
            PricingMode mode = PricingMode.fromString(modeRaw, hasTiers);
            if (mode == PricingMode.FLAT && hasTiers) {
                throw new IllegalArgumentException("pricing_mode=flat cannot have tiers");
            }
            if ((mode == PricingMode.VOLUME || mode == PricingMode.GRADUATED) && !hasTiers) {
                throw new IllegalArgumentException("pricing_mode=" + modeRaw + " requires tiers");
            }

            return new ModelPricing(mode, input, output, embedding, hasTiers ? tiers : null);
        }

        long calculateEventCostMicro(TokenEvent event, long monthlyTokensBefore, double cacheReadMultiplier) {
            return switch (pricingMode) {
                case FLAT -> costAtTier(event, selectTier(monthlyTokensBefore), cacheReadMultiplier);
                case VOLUME -> costAtTier(event, selectTier(monthlyTokensBefore), cacheReadMultiplier);
                case GRADUATED -> costGraduated(event, monthlyTokensBefore, cacheReadMultiplier);
            };
        }

        /** Volume meter uses total_tokens; categories share one cursor across the event. */
        private long costGraduated(TokenEvent event, long monthlyTokensBefore, double cacheReadMultiplier) {
            long cursor = monthlyTokensBefore;
            long cost = 0;
            cost += costCategoryGraduated(event.getInputTokens(), cursor, cacheReadMultiplier, Category.INPUT);
            cursor += event.getInputTokens();
            cost += costCategoryGraduated(event.getOutputTokens(), cursor, cacheReadMultiplier, Category.OUTPUT);
            cursor += event.getOutputTokens();
            cost += costCategoryGraduated(event.getCacheReadTokens(), cursor, cacheReadMultiplier, Category.CACHE_READ);
            cursor += event.getCacheReadTokens();
            cost += costCategoryGraduated(event.getReasoningTokens(), cursor, cacheReadMultiplier, Category.REASONING);
            cursor += event.getReasoningTokens();
            cost += costCategoryGraduated(event.getCacheWriteTokens(), cursor, cacheReadMultiplier, Category.CACHE_WRITE);
            cursor += event.getCacheWriteTokens();
            cost += costCategoryGraduated(event.getEmbeddingTokens(), cursor, cacheReadMultiplier, Category.EMBEDDING);
            return cost;
        }

        private long costCategoryGraduated(
                long tokens, long cursor, double cacheReadMultiplier, Category category) {
            long remaining = tokens;
            long cost = 0;
            while (remaining > 0) {
                Tier tier = tierAtToken(cursor);
                long tierEnd = tierEndTokens(tier);
                long capacity = tierEnd == Long.MAX_VALUE ? remaining : Math.min(remaining, tierEnd - cursor);
                if (capacity <= 0) {
                    // ponytail: guard against misconfigured tiers; fall through to last tier
                    tier = tiers.get(tiers.size() - 1);
                    capacity = remaining;
                }
                cost += Math.round(capacity * rateFor(tier, category, cacheReadMultiplier));
                cursor += capacity;
                remaining -= capacity;
            }
            return cost;
        }

        private long costAtTier(TokenEvent event, Tier tier, double cacheReadMultiplier) {
            long cost = 0;
            cost += Math.round(event.getInputTokens() * tier.inputPerM);
            cost += Math.round(event.getOutputTokens() * tier.outputPerM);
            cost += Math.round(event.getCacheReadTokens() * tier.inputPerM * cacheReadMultiplier);
            cost += Math.round(event.getReasoningTokens() * tier.outputPerM);
            cost += Math.round(event.getCacheWriteTokens() * tier.inputPerM);
            cost += Math.round(event.getEmbeddingTokens() * tier.embeddingPerM);
            return cost;
        }

        private static double rateFor(Tier tier, Category category, double cacheReadMultiplier) {
            return switch (category) {
                case INPUT -> tier.inputPerM;
                case OUTPUT, REASONING -> tier.outputPerM;
                case CACHE_READ -> tier.inputPerM * cacheReadMultiplier;
                case CACHE_WRITE -> tier.inputPerM;
                case EMBEDDING -> tier.embeddingPerM;
            };
        }

        Tier selectTier(long monthlyTokens) {
            if (tiers.isEmpty()) {
                return new Tier(null, inputPerM, outputPerM, embeddingPerM);
            }
            return tierAtToken(monthlyTokens);
        }

        /** Tier containing token index {@code tokenIndex} (0-based cumulative volume). */
        Tier tierAtToken(long tokenIndex) {
            if (tiers.isEmpty()) {
                return new Tier(null, inputPerM, outputPerM, embeddingPerM);
            }
            long tokensM = tokenIndex / 1_000_000L;
            for (Tier tier : tiers) {
                if (tier.upToTokensM == null || tokensM < tier.upToTokensM) {
                    return tier;
                }
            }
            return tiers.get(tiers.size() - 1);
        }

        /** Exclusive upper bound in raw tokens for {@code tier}. */
        static long tierEndTokens(Tier tier) {
            if (tier.upToTokensM == null) {
                return Long.MAX_VALUE;
            }
            return tier.upToTokensM * 1_000_000L;
        }

        Map<String, Object> toMap() {
            Map<String, Object> m = new HashMap<>();
            if (pricingMode != PricingMode.FLAT) {
                m.put("pricing_mode", pricingMode.name().toLowerCase());
            }
            m.put("input_per_m", inputPerM);
            m.put("output_per_m", outputPerM);
            m.put("embedding_per_m", embeddingPerM);
            if (!tiers.isEmpty()) {
                List<Map<String, Object>> tierList = new ArrayList<>();
                for (Tier t : tiers) {
                    Map<String, Object> tm = new HashMap<>();
                    tm.put("up_to_tokens_m", t.upToTokensM);
                    tm.put("input_per_m", t.inputPerM);
                    tm.put("output_per_m", t.outputPerM);
                    tm.put("embedding_per_m", t.embeddingPerM);
                    tierList.add(tm);
                }
                m.put("tiers", tierList);
            }
            return m;
        }
    }

    private enum Category {
        INPUT, OUTPUT, CACHE_READ, REASONING, CACHE_WRITE, EMBEDDING
    }

    public static final class Tier {
        final Long upToTokensM;
        final double inputPerM;
        final double outputPerM;
        final double embeddingPerM;

        Tier(Long upToTokensM, double inputPerM, double outputPerM, double embeddingPerM) {
            this.upToTokensM = upToTokensM;
            this.inputPerM = inputPerM;
            this.outputPerM = outputPerM;
            this.embeddingPerM = embeddingPerM;
        }
    }
}
