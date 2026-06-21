import io.fluxmeter.model.TokenEvent;
import io.fluxmeter.model.UsageAggregate;

public class PricingCheck {
    public static void main(String[] args) {
        TokenEvent e = new TokenEvent();
        e.setModelId("gpt-4o-mini");
        e.setInputTokens(1000);
        long micro = UsageAggregate.calculateEventCostMicro(e);
        System.out.println("gpt-4o-mini 1000 input microdollars=" + micro);
        if (micro != 150) {
            System.err.println("FAIL expected 150");
            System.exit(1);
        }

        e.setModelId("gpt-4o-2024-08-06");
        e.setInputTokens(1000);
        micro = UsageAggregate.calculateEventCostMicro(e);
        System.out.println("gpt-4o-2024-08-06 1000 input microdollars=" + micro);
        if (micro != 2500) {
            System.err.println("FAIL expected 2500");
            System.exit(1);
        }

        e.setModelId("gemini-1.5-flash");
        e.setInputTokens(1_000_000);
        micro = UsageAggregate.calculateEventCostMicro(e);
        System.out.println("gemini-1.5-flash 1M input microdollars=" + micro);
        if (micro != 75_000) {
            System.err.println("FAIL expected 75000");
            System.exit(1);
        }

        System.out.println("PricingCheck PASSED");
    }
}
