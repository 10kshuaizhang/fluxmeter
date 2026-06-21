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
import java.util.Set;

/**
 * External pricing catalog loaded from JSON file or Redis snapshot.
 * Replaces hardcoded prices in UsageAggregate.
 */
public final class PricingCatalog {

    private static volatile PricingCatalog INSTANCE = PricingCatalog.loadDefault();

    private final Map<String, ModelPricing> models;
    private final ModelPricing defaults;
    private final List<String> prefixModels;
    private final double cacheReadMultiplier;
    private final String version;

    private PricingCatalog(
            Map<String, ModelPricing> models,
            ModelPricing defaults,
            List<String> prefixModels,
            double cacheReadMultiplier,
            String version) {
        this.models = models;
        this.defaults = defaults;
        this.prefixModels = prefixModels;
        this.cacheReadMultiplier = cacheReadMultiplier;
        this.version = version;
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
            defaults = new ModelPricing(1.0, 3.0, 0.1, null);
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
        return new PricingCatalog(models, defaults, prefixModels, cacheMult, version);
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

  /** Cost in microdollars using flat rates (monthly volume for tiers in v2). */
    public long calculateEventCostMicro(TokenEvent event) {
        return calculateEventCostMicro(event, 0L);
    }

    public long calculateEventCostMicro(TokenEvent event, long monthlyTokensBefore) {
        String model = normalizeModelId(event.getModelId());
        ModelPricing pricing = models.getOrDefault(model, defaults);
        Tier tier = pricing.selectTier(monthlyTokensBefore);

        long cost = 0;
        cost += Math.round(event.getInputTokens() * tier.inputPerM);
        cost += Math.round(event.getOutputTokens() * tier.outputPerM);
        cost += Math.round(event.getCacheReadTokens() * tier.inputPerM * cacheReadMultiplier);
        cost += Math.round(event.getReasoningTokens() * tier.outputPerM);
        cost += Math.round(event.getCacheWriteTokens() * tier.inputPerM);
        cost += Math.round(event.getEmbeddingTokens() * tier.embeddingPerM);
        return cost;
    }

    public double calculateEventCost(TokenEvent event) {
        return calculateEventCostMicro(event) / 1_000_000.0;
    }

    public String toJsonSnapshot() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> out = new HashMap<>();
        out.put("version", version);
        out.put("cache_read_multiplier", cacheReadMultiplier);
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
        List<String> prefixes = List.of(
                "gpt-4o-mini", "gpt-4o", "o3-mini", "o1",
                "claude-opus-4", "claude-sonnet-4", "claude-haiku-4",
                "gemini-1.5-pro", "gemini-1.5-flash",
                "text-embedding-3-large", "text-embedding-3-small");
        return new PricingCatalog(
                models,
                new ModelPricing(1.0, 3.0, 0.1, null),
                prefixes,
                0.5,
                "builtin");
    }

    public static final class ModelPricing {
        private final double inputPerM;
        private final double outputPerM;
        private final double embeddingPerM;
        private final List<Tier> tiers;

        public ModelPricing(double inputPerM, double outputPerM, double embeddingPerM, List<Tier> tiers) {
            this.inputPerM = inputPerM;
            this.outputPerM = outputPerM;
            this.embeddingPerM = embeddingPerM;
            this.tiers = tiers != null ? tiers : Collections.emptyList();
        }

        static ModelPricing flat(double input, double output, double embedding) {
            return new ModelPricing(input, output, embedding, null);
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
            return new ModelPricing(input, output, embedding, tiers.isEmpty() ? null : tiers);
        }

        Tier selectTier(long monthlyTokens) {
            if (tiers.isEmpty()) {
                return new Tier(null, inputPerM, outputPerM, embeddingPerM);
            }
            long tokensM = monthlyTokens / 1_000_000L;
            for (Tier tier : tiers) {
                if (tier.upToTokensM == null || tokensM < tier.upToTokensM) {
                    return tier;
                }
            }
            return tiers.get(tiers.size() - 1);
        }

        Map<String, Object> toMap() {
            Map<String, Object> m = new HashMap<>();
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
