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

    @Test
    void windowReservationsKey() {
        assertEquals("window:reservations:c|m|1", TenantKeys.windowReservationsKey("c|m|1"));
    }

    @Test
    void scopePrefixIsTenantScoped() {
        assertEquals("span:s1", TenantKeys.scopePrefix(null, "span", "s1"));
        assertEquals("tenant:t1:session:s1", TenantKeys.scopePrefix("t1", "session", "s1"));
    }

    @Test
    void packageKeyIsTenantScoped() {
        assertEquals("package:c1:tokens_remaining", TenantKeys.packageKey(null, "c1"));
        assertEquals("tenant:t1:package:c1:tokens_remaining", TenantKeys.packageKey("t1", "c1"));
    }
}
