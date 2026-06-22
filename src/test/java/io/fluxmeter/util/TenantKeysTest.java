package io.fluxmeter.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TenantKeysTest {

    @Test
    void singleTenant_customerPrefix() {
        assertEquals("customer:cust_1", TenantKeys.customerPrefix(null, "cust_1"));
        assertEquals("customer:cust_1", TenantKeys.customerPrefix("", "cust_1"));
    }

    @Test
    void multiTenant_customerPrefix() {
        assertEquals("tenant:t1:customer:cust_1", TenantKeys.customerPrefix("t1", "cust_1"));
    }

    @Test
    void budgetPrefix() {
        assertEquals("budget:cust_1", TenantKeys.budgetPrefix(null, "cust_1"));
        assertEquals("tenant:t1:budget:cust_1", TenantKeys.budgetPrefix("t1", "cust_1"));
    }

    @Test
    void globalKey() {
        assertEquals("global:total_tokens", TenantKeys.globalKey(null, "total_tokens"));
        assertEquals("tenant:t1:global:total_tokens", TenantKeys.globalKey("t1", "total_tokens"));
    }

    @Test
    void windowId_includesTenantWhenPresent() {
        assertEquals("c|m|100", TenantKeys.windowId(null, "c", "m", 100));
        assertEquals("t|c|m|100", TenantKeys.windowId("t", "c", "m", 100));
    }

    @Test
    void hasTenant() {
        assertFalse(TenantKeys.hasTenant(null));
        assertFalse(TenantKeys.hasTenant("  "));
        assertTrue(TenantKeys.hasTenant("tenant_1"));
    }
}
