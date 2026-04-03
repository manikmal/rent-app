import React, { useEffect, useMemo, useRef, useState } from "react";
import "./styles.css";

const API_BASE = process.env.REACT_APP_API_BASE || "/api";

const STATUS_OPTIONS = [
  { value: "ALL", label: "All statuses" },
  { value: "PENDING", label: "Pending" },
  { value: "PAID", label: "Paid" },
  { value: "PARTIALLY_PAID", label: "Partially paid" },
  { value: "LATE", label: "Late" },
  { value: "SURPLUS", label: "Surplus" },
];

const SORT_OPTIONS = [
  { value: "tenant_asc", label: "Tenant A-Z" },
  { value: "tenant_desc", label: "Tenant Z-A" },
  { value: "outstanding_desc", label: "Highest balance" },
  { value: "rent_desc", label: "Highest rent" },
  { value: "due_soon", label: "Due soon" },
  { value: "recent_payment", label: "Recent payment" },
];

const createRentIncreaseForm = () => ({
  id: null,
  dateFrom: "",
  dateTill: "",
  rentAmount: "",
});

const emptyTenantForm = {
  id: null,
  tenantName: "",
  propertyName: "",
  rentAmount: "",
  rentDueDay: "",
  leaseStart: "",
  leaseEnd: "",
  phoneNumber: "",
  unitNumber: "",
  propertyAddress: "",
  securityDeposit: "",
  leaseTerms: "",
  emergencyContactName: "",
  emergencyContactPhone: "",
  rentIncreases: [],
};

const emptyHistoryFilter = {
  from: "",
  to: "",
};

const emptyLoginForm = {
  username: "",
  password: "",
};

function currentMonthValue() {
  const now = new Date();
  return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`;
}

function toTenantForm(property) {
  return {
    id: property?.id ?? null,
    tenantName: property?.tenant_name ?? "",
    propertyName: property?.property_name ?? "",
    rentAmount: property?.rent_amount ?? "",
    rentDueDay: property?.rent_due_day ?? "",
    leaseStart: property?.lease_start ?? "",
    leaseEnd: property?.lease_end ?? "",
    phoneNumber: property?.phone_number ?? "",
    unitNumber: property?.unit_number ?? "",
    propertyAddress: property?.property_address ?? "",
    securityDeposit: property?.security_deposit ?? "",
    leaseTerms: property?.lease_terms ?? "",
    emergencyContactName: property?.emergency_contact_name ?? "",
    emergencyContactPhone: property?.emergency_contact_phone ?? "",
    rentIncreases:
      property?.rent_increases?.map((item) => ({
        id: item.id ?? null,
        dateFrom: item.date_from ?? "",
        dateTill: item.date_till ?? "",
        rentAmount: item.rent_amount ?? "",
      })) ?? [],
  };
}

async function apiRequest(path, options = {}) {
  const response = await fetch(`${API_BASE}${path}`, {
    credentials: "include",
    ...options,
  });
  const text = await response.text();
  let data = null;

  if (text) {
    try {
      data = JSON.parse(text);
    } catch (error) {
      data = null;
    }
  }

  if (!response.ok) {
    const message = data?.detail || data?.message || text || "Request failed.";
    const error = new Error(message);
    error.status = response.status;
    throw error;
  }

  return data;
}

function formatCurrency(value) {
  return new Intl.NumberFormat("en-IN", {
    style: "currency",
    currency: "INR",
    maximumFractionDigits: 2,
  }).format(Number(value || 0));
}

function formatDate(value) {
  if (!value) {
    return "Not set";
  }
  const parsed = new Date(`${value}T00:00:00`);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat("en-IN", {
    day: "numeric",
    month: "short",
    year: "numeric",
  }).format(parsed);
}

function formatMonthLabel(value) {
  if (!value) {
    return "";
  }
  const parsed = new Date(`${value}-01T00:00:00`);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat("en-IN", {
    month: "long",
    year: "numeric",
  }).format(parsed);
}

function severityClass(severity) {
  return severity ? `severity-${severity}` : "severity-info";
}

function metricRows(metrics) {
  if (!metrics) {
    return [];
  }
  return [
    { label: "Expected", value: formatCurrency(metrics.expected), tone: "sand" },
    { label: "Collected", value: formatCurrency(metrics.collected), tone: "mint" },
    { label: "Outstanding", value: formatCurrency(metrics.outstanding), tone: "amber" },
    { label: "Surplus", value: formatCurrency(metrics.surplus), tone: "blue" },
    { label: "Unmatched", value: String(metrics.unmatched || 0), tone: "coral" },
    { label: "Needs attention", value: String(metrics.needs_attention || 0), tone: "ink" },
  ];
}

function App() {
  const [authChecked, setAuthChecked] = useState(false);
  const [currentUser, setCurrentUser] = useState(null);
  const [loginForm, setLoginForm] = useState(emptyLoginForm);
  const [loginLoading, setLoginLoading] = useState(false);
  const [selectedMonth, setSelectedMonth] = useState(currentMonthValue);
  const [searchQuery, setSearchQuery] = useState("");
  const [statusFilter, setStatusFilter] = useState("ALL");
  const [sortBy, setSortBy] = useState("tenant_asc");
  const [properties, setProperties] = useState([]);
  const [dashboard, setDashboard] = useState(null);
  const [unmatchedPayments, setUnmatchedPayments] = useState([]);
  const [selectedTenantId, setSelectedTenantId] = useState(null);
  const [selectedTenantDetail, setSelectedTenantDetail] = useState(null);
  const [tenantHistoryFilter, setTenantHistoryFilter] = useState(emptyHistoryFilter);
  const [tenantForm, setTenantForm] = useState(emptyTenantForm);
  const [showTenantForm, setShowTenantForm] = useState(false);
  const [paymentMessage, setPaymentMessage] = useState("");
  const [paymentResult, setPaymentResult] = useState(null);
  const [manualSelections, setManualSelections] = useState({});
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [loadingPage, setLoadingPage] = useState(true);
  const [processingPayment, setProcessingPayment] = useState(false);
  const [savingTenant, setSavingTenant] = useState(false);
  const [detailLoading, setDetailLoading] = useState(false);
  const [deletingTenantId, setDeletingTenantId] = useState(null);
  const [busyReviewIds, setBusyReviewIds] = useState([]);
  const [busyUndoIds, setBusyUndoIds] = useState([]);
  const editorRef = useRef(null);

  const metrics = useMemo(() => metricRows(dashboard?.metrics), [dashboard]);

  const selectedTenantSummary =
    selectedTenantDetail?.property ||
    properties.find((property) => property.id === selectedTenantId) ||
    null;

  const setBusyFlag = (setter, id, active) => {
    setter((current) =>
      active ? [...new Set([...current, id])] : current.filter((item) => item !== id)
    );
  };

  const checkAuth = async () => {
    try {
      const user = await apiRequest("/auth/me");
      setCurrentUser(user.username);
      setError("");
    } catch (requestError) {
      if (requestError.status === 401) {
        setCurrentUser(null);
      } else {
        setError(requestError.message);
      }
    } finally {
      setAuthChecked(true);
    }
  };

  const loadData = async () => {
    setLoadingPage(true);
    try {
      const query = new URLSearchParams({
        month: selectedMonth,
        q: searchQuery,
        status: statusFilter,
        sort: sortBy,
      });

      const [dashboardData, propertiesData, unmatchedData] = await Promise.all([
        apiRequest(`/dashboard?month=${selectedMonth}`),
        apiRequest(`/properties?${query.toString()}`),
        apiRequest("/unmatched-payments?status=UNMATCHED"),
      ]);

      setDashboard(dashboardData);
      setProperties(propertiesData);
      setUnmatchedPayments(unmatchedData);
      setError("");

      if (selectedTenantId && !propertiesData.some((property) => property.id === selectedTenantId)) {
        setSelectedTenantId(null);
        setSelectedTenantDetail(null);
      }
    } catch (requestError) {
      if (requestError.status === 401) {
        setCurrentUser(null);
        setAuthChecked(true);
        setSelectedTenantId(null);
        setSelectedTenantDetail(null);
        return;
      }
      setError(requestError.message);
    } finally {
      setLoadingPage(false);
    }
  };

  const loadTenantDetail = async (propertyId) => {
    setDetailLoading(true);
    try {
      const query = new URLSearchParams({ month: selectedMonth });
      if (tenantHistoryFilter.from) {
        query.set("history_from", tenantHistoryFilter.from);
      }
      if (tenantHistoryFilter.to) {
        query.set("history_to", tenantHistoryFilter.to);
      }
      const detail = await apiRequest(`/properties/${propertyId}/ledger?${query.toString()}`);
      setSelectedTenantDetail(detail);
      setError("");
    } catch (requestError) {
      if (requestError.status === 401) {
        setCurrentUser(null);
        setAuthChecked(true);
        return;
      }
      setError(requestError.message);
    } finally {
      setDetailLoading(false);
    }
  };

  useEffect(() => {
    checkAuth();
  }, []);

  useEffect(() => {
    if (!currentUser) {
      return;
    }
    loadData();
  }, [currentUser, selectedMonth, searchQuery, statusFilter, sortBy]);

  useEffect(() => {
    if (currentUser && selectedTenantId) {
      loadTenantDetail(selectedTenantId);
    }
  }, [currentUser, selectedTenantId, selectedMonth, tenantHistoryFilter.from, tenantHistoryFilter.to]);

  useEffect(() => {
    if (showTenantForm && editorRef.current) {
      editorRef.current.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  }, [showTenantForm]);

  const resetTenantEditor = () => {
    setTenantForm(emptyTenantForm);
    setShowTenantForm(false);
  };

  const openNewTenantForm = () => {
    setError("");
    setNotice("");
    setTenantForm(emptyTenantForm);
    setShowTenantForm(true);
  };

  const startEditingTenant = (property) => {
    setError("");
    setNotice("");
    setTenantForm(toTenantForm(property));
    setShowTenantForm(true);
  };

  const openTenantDetails = (propertyId) => {
    setTenantHistoryFilter(emptyHistoryFilter);
    setSelectedTenantId(propertyId);
  };

  const closeTenantDetails = () => {
    setSelectedTenantId(null);
    setSelectedTenantDetail(null);
    setTenantHistoryFilter(emptyHistoryFilter);
  };

  const updateRentIncreaseField = (index, field, value) => {
    setTenantForm((current) => ({
      ...current,
      rentIncreases: current.rentIncreases.map((item, itemIndex) =>
        itemIndex === index ? { ...item, [field]: value } : item
      ),
    }));
  };

  const addRentIncrease = () => {
    setTenantForm((current) => ({
      ...current,
      rentIncreases: [...current.rentIncreases, createRentIncreaseForm()],
    }));
  };

  const removeRentIncrease = (index) => {
    setTenantForm((current) => ({
      ...current,
      rentIncreases: current.rentIncreases.filter((_, itemIndex) => itemIndex !== index),
    }));
  };

  const handleTenantSave = async (event) => {
    event.preventDefault();
    setSavingTenant(true);
    setError("");
    setNotice("");

    try {
      const payload = {
        tenant_name: tenantForm.tenantName,
        property_name: tenantForm.propertyName,
        rent_amount: Number(tenantForm.rentAmount),
        rent_due_day: tenantForm.rentDueDay ? Number(tenantForm.rentDueDay) : null,
        lease_start: tenantForm.leaseStart || null,
        lease_end: tenantForm.leaseEnd || null,
        phone_number: tenantForm.phoneNumber || null,
        unit_number: tenantForm.unitNumber || null,
        property_address: tenantForm.propertyAddress || null,
        security_deposit: tenantForm.securityDeposit ? Number(tenantForm.securityDeposit) : null,
        lease_terms: tenantForm.leaseTerms || null,
        emergency_contact_name: tenantForm.emergencyContactName || null,
        emergency_contact_phone: tenantForm.emergencyContactPhone || null,
        rent_increases: tenantForm.rentIncreases.map((item) => ({
          date_from: item.dateFrom,
          date_till: item.dateTill,
          rent_amount: Number(item.rentAmount),
        })),
      };

      const isEditing = Boolean(tenantForm.id);
      const savedProperty = await apiRequest(
        isEditing ? `/properties/${tenantForm.id}` : "/properties",
        {
          method: isEditing ? "PUT" : "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        }
      );

      setNotice(isEditing ? "Tenant updated." : "Tenant added.");
      resetTenantEditor();
      await loadData();

      if (selectedTenantId === savedProperty.id || !selectedTenantId) {
        setSelectedTenantId(savedProperty.id);
      }
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setSavingTenant(false);
    }
  };

  const handleDeleteTenant = async (property) => {
    if (!window.confirm(`Delete ${property.tenant_name}?`)) {
      return;
    }

    setDeletingTenantId(property.id);
    setError("");
    setNotice("");

    try {
      await apiRequest(`/properties/${property.id}`, { method: "DELETE" });
      setNotice("Tenant deleted.");

      if (tenantForm.id === property.id) {
        resetTenantEditor();
      }
      if (selectedTenantId === property.id) {
        setSelectedTenantId(null);
        setSelectedTenantDetail(null);
      }
      await loadData();
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setDeletingTenantId(null);
    }
  };

  const handlePaymentProcess = async () => {
    setProcessingPayment(true);
    setError("");
    setNotice("");
    setPaymentResult(null);

    try {
      const result = await apiRequest("/process-payment", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: paymentMessage }),
      });
      setPaymentResult(result);
      setPaymentMessage("");
      setNotice(result.status === "UNMATCHED" ? "Payment sent to review inbox." : "Payment processed.");
      await loadData();
      if (result.matched_property?.id) {
        setSelectedTenantId(result.matched_property.id);
      }
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setProcessingPayment(false);
    }
  };

  const handleLogin = async (event) => {
    event.preventDefault();
    setLoginLoading(true);
    setError("");

    try {
      const user = await apiRequest("/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(loginForm),
      });
      setCurrentUser(user.username);
      setLoginForm(emptyLoginForm);
      setNotice("Signed in.");
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setLoginLoading(false);
      setAuthChecked(true);
    }
  };

  const handleLogout = async () => {
    try {
      await apiRequest("/auth/logout", { method: "POST" });
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setCurrentUser(null);
      setDashboard(null);
      setProperties([]);
      setUnmatchedPayments([]);
      setSelectedTenantId(null);
      setSelectedTenantDetail(null);
      setPaymentResult(null);
      setNotice("");
    }
  };

  const handleInboxMatch = async (payment, selectionKey) => {
    const propertyId = manualSelections[selectionKey];
    if (!propertyId) {
      setError("Please select a tenant to match this payment.");
      return;
    }

    const unmatchedPaymentId = payment.id || payment.unmatched_payment_id;
    setBusyFlag(setBusyReviewIds, unmatchedPaymentId, true);
    setError("");
    setNotice("");

    try {
      const result = await apiRequest(`/unmatched-payments/${unmatchedPaymentId}/match`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          property_id: Number(propertyId),
          sender_key: payment.sender_key || null,
        }),
      });

      setManualSelections((current) => ({ ...current, [selectionKey]: "" }));
      setPaymentResult(result);
      setNotice("Payment matched.");
      setSelectedTenantId(result.matched_property.id);
      await loadData();
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setBusyFlag(setBusyReviewIds, unmatchedPaymentId, false);
    }
  };

  const handleInboxDecision = async (paymentId, action) => {
    const label = action === "reject" ? "reject" : "mark as duplicate";
    if (!window.confirm(`Do you want to ${label} this payment?`)) {
      return;
    }

    setBusyFlag(setBusyReviewIds, paymentId, true);
    setError("");
    setNotice("");

    try {
      await apiRequest(`/unmatched-payments/${paymentId}/${action}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      });
      setNotice(
        action === "reject" ? "Payment marked as rejected." : "Payment marked as duplicate."
      );
      await loadData();
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setBusyFlag(setBusyReviewIds, paymentId, false);
    }
  };

  const handleUndoPayment = async (paymentId) => {
    if (!window.confirm("Undo this payment? It will reopen the inbox item if one was matched.")) {
      return;
    }

    setBusyFlag(setBusyUndoIds, paymentId, true);
    setError("");
    setNotice("");

    try {
      const result = await apiRequest(`/payments/${paymentId}/undo`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      });
      setNotice("Payment undone.");
      setSelectedTenantId(result.property_id);
      await loadData();
      await loadTenantDetail(result.property_id);
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setBusyFlag(setBusyUndoIds, paymentId, false);
    }
  };

  const renderPaymentMatcher = (payment, selectionKey) => {
    const unmatchedPaymentId = payment.id || payment.unmatched_payment_id;
    const isBusy = busyReviewIds.includes(unmatchedPaymentId);

    return (
      <div className="review-actions">
        <div className="matcher-inline">
          <select
            value={manualSelections[selectionKey] || ""}
            onChange={(event) =>
              setManualSelections((current) => ({
                ...current,
                [selectionKey]: event.target.value,
              }))
            }
            disabled={isBusy}
          >
            <option value="">Choose tenant</option>
            {(payment.candidates?.length > 0 ? payment.candidates : properties).map((property) => (
              <option key={property.id} value={property.id}>
                {property.tenant_name} • {property.property_name || "Property not set"} •{" "}
                {formatCurrency(property.current_rent_amount || property.rent_amount)}
              </option>
            ))}
          </select>
          <button
            type="button"
            onClick={() => handleInboxMatch(payment, selectionKey)}
            disabled={isBusy}
          >
            {isBusy ? "Saving..." : "Approve Match"}
          </button>
        </div>

        <div className="inline-actions">
          <button
            type="button"
            className="ghost-button"
            onClick={() => handleInboxDecision(unmatchedPaymentId, "duplicate")}
            disabled={isBusy}
          >
            Duplicate
          </button>
          <button
            type="button"
            className="ghost-danger"
            onClick={() => handleInboxDecision(unmatchedPaymentId, "reject")}
            disabled={isBusy}
          >
            Reject
          </button>
        </div>
      </div>
    );
  };

  if (!authChecked) {
    return (
      <div className="app-shell auth-shell">
        <section className="surface auth-card">
          <div className="brand-lockup auth-brand-lockup">
            <img src="/rent-management-mark.svg" alt="Rent Management" className="brand-mark" />
            <div>
              <p className="eyebrow">Rent Management</p>
            </div>
          </div>
          <h1>Checking your session...</h1>
        </section>
      </div>
    );
  }

  if (!currentUser) {
    return (
      <div className="app-shell auth-shell">
        <section className="surface auth-card">
          <div className="brand-lockup auth-brand-lockup">
            <img src="/rent-management-mark.svg" alt="Rent Management" className="brand-mark" />
            <div>
              <p className="eyebrow">Rent Management</p>
            </div>
          </div>
          <h1>Sign in to manage collections.</h1>
          <p className="hero-copy">
            This small production setup is protected with account-based login before the dashboard loads.
          </p>

          {error && (
            <div className="banner error" role="alert">
              {error}
            </div>
          )}

          <form className="editor-form" onSubmit={handleLogin}>
            <label className="field">
              Username
              <input
                value={loginForm.username}
                onChange={(event) =>
                  setLoginForm((current) => ({ ...current, username: event.target.value }))
                }
                autoComplete="username"
                required
              />
            </label>

            <label className="field">
              Password
              <input
                type="password"
                value={loginForm.password}
                onChange={(event) =>
                  setLoginForm((current) => ({ ...current, password: event.target.value }))
                }
                autoComplete="current-password"
                required
              />
            </label>

            <button type="submit" className="primary-wide" disabled={loginLoading}>
              {loginLoading ? "Signing in..." : "Sign in"}
            </button>
          </form>
        </section>
      </div>
    );
  }

  return (
    <div className="app-shell">
      <header className="hero">
        <div className="hero-copy-wrap">
          <div className="brand-lockup hero-brand-lockup">
            <img src="/rent-management-mark.svg" alt="Rent Management" className="brand-mark" />
            <div>
              <p className="eyebrow">Rent Management</p>
            </div>
          </div>
          <h1>Collections, reminders, and review work in one place.</h1>
          <p className="hero-copy">
            Track rent month by month, review unmatched credits safely, and keep each tenant’s
            payment history close at hand.
          </p>
        </div>

        <div className="hero-actions">
          <label className="field month-field">
            Reporting month
            <input
              type="month"
              value={selectedMonth}
              onChange={(event) => setSelectedMonth(event.target.value)}
            />
          </label>
          <div className="hero-user">
            <span>Signed in as {currentUser}</span>
            <button type="button" className="ghost-button" onClick={handleLogout}>
              Sign out
            </button>
          </div>
          <button type="button" onClick={openNewTenantForm} disabled={savingTenant}>
            Add Tenant
          </button>
        </div>
      </header>

      {error && (
        <div className="banner error" role="alert">
          {error}
        </div>
      )}
      {notice && (
        <div className="banner success" role="status">
          {notice}
        </div>
      )}

      <section className="metrics-grid">
        {metrics.map((metric) => (
          <article key={metric.label} className={`metric-card tone-${metric.tone}`}>
            <p>{metric.label}</p>
            <strong>{metric.value}</strong>
            <span>{formatMonthLabel(selectedMonth)}</span>
          </article>
        ))}
      </section>

      <section className="insight-grid">
        <section className="surface insight-panel">
          <div className="section-head">
            <div>
              <p className="section-kicker">Reminders</p>
              <h2>Upcoming and overdue work</h2>
            </div>
          </div>

          {dashboard?.reminders?.length ? (
            <div className="reminder-list">
              {dashboard.reminders.map((item, index) => (
                <article key={`${item.type}-${index}`} className={`reminder-card ${severityClass(item.severity)}`}>
                  <strong>{item.title}</strong>
                  <p>{item.description}</p>
                </article>
              ))}
            </div>
          ) : (
            <div className="empty-note">No reminders for this month.</div>
          )}
        </section>

        <section className="surface insight-panel">
          <div className="section-head">
            <div>
              <p className="section-kicker">Trends</p>
              <h2>Recent collection run</h2>
            </div>
          </div>

          {dashboard?.trends?.length ? (
            <div className="trend-list">
              {dashboard.trends.map((item) => {
                const percent = item.expected ? Math.min((item.collected / item.expected) * 100, 100) : 0;
                return (
                  <article key={item.month} className="trend-card">
                    <div className="trend-head">
                      <strong>{formatMonthLabel(item.month)}</strong>
                      <span>{formatCurrency(item.collected)}</span>
                    </div>
                    <div className="trend-bar">
                      <span style={{ width: `${percent}%` }} />
                    </div>
                    <p>
                      Expected {formatCurrency(item.expected)} • Outstanding {formatCurrency(item.outstanding)} •
                      Unmatched {item.unmatched}
                    </p>
                  </article>
                );
              })}
            </div>
          ) : (
            <div className="empty-note">Trend data will appear as months accumulate.</div>
          )}
        </section>
      </section>

      <main className="dashboard-grid">
        <section className="surface command-center">
          <div className="section-head">
            <div>
              <p className="section-kicker">Payment Desk</p>
              <h2>Process incoming payment text</h2>
            </div>
          </div>

          <label className="field">
            Bank message
            <textarea
              rows="6"
              value={paymentMessage}
              onChange={(event) => setPaymentMessage(event.target.value)}
              placeholder="Paste the credited NEFT message here"
            />
          </label>

          <button
            type="button"
            className="primary-wide"
            onClick={handlePaymentProcess}
            disabled={processingPayment || !paymentMessage.trim()}
          >
            {processingPayment ? "Processing..." : "Process Payment"}
          </button>

          {paymentResult ? (
            <div className="payment-card">
              <div className="payment-topline">
                <span className={`status-pill status-${paymentResult.status.toLowerCase()}`}>
                  {paymentResult.status.replaceAll("_", " ")}
                </span>
                {paymentResult.amount != null && <strong>{formatCurrency(paymentResult.amount)}</strong>}
              </div>

              {paymentResult.matched_property ? (
                <div className="payment-summary">
                  <p>{paymentResult.matched_property.tenant_name}</p>
                  <span>{paymentResult.matched_property.property_name || "Property not set"}</span>
                  <span>
                    Paid this month: {formatCurrency(paymentResult.current_month_paid_amount)}
                  </span>
                  <span>Balance: {formatCurrency(paymentResult.balance_amount)}</span>
                  <span>Surplus: {formatCurrency(paymentResult.surplus_amount)}</span>
                </div>
              ) : (
                <div className="payment-summary">
                  <p>Review needed</p>
                  <span>
                    {paymentResult.matching_hint || "We could not confidently identify this payment."}
                  </span>
                </div>
              )}

              {paymentResult.status === "UNMATCHED" &&
                renderPaymentMatcher(
                  paymentResult,
                  `payment-result-${paymentResult.unmatched_payment_id}`
                )}
            </div>
          ) : (
            <div className="empty-note">
              Run a bank message through the matcher to post it directly or send it into review.
            </div>
          )}
        </section>

        <section className="surface roster-surface">
          <div className="section-head">
            <div>
              <p className="section-kicker">Roster</p>
              <h2>Tenant accounts</h2>
            </div>
          </div>

          <div className="filter-grid">
            <label className="field">
              Search
              <input
                value={searchQuery}
                onChange={(event) => setSearchQuery(event.target.value)}
                placeholder="Tenant, property, unit, phone"
              />
            </label>

            <label className="field">
              Status
              <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
                {STATUS_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>

            <label className="field">
              Sort
              <select value={sortBy} onChange={(event) => setSortBy(event.target.value)}>
                {SORT_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
          </div>

          {showTenantForm && (
            <section ref={editorRef} className="surface editor-surface editor-surface-inline">
              <div className="section-head">
                <div>
                  <p className="section-kicker">Editor</p>
                  <h2>{tenantForm.id ? "Edit tenant" : "Add tenant"}</h2>
                </div>
                <button type="button" className="ghost-button" onClick={resetTenantEditor} disabled={savingTenant}>
                  Close
                </button>
              </div>

              <form onSubmit={handleTenantSave} className="editor-form">
                <div className="form-grid">
                  <label className="field">
                    Tenant name
                    <input
                      value={tenantForm.tenantName}
                      onChange={(event) =>
                        setTenantForm((current) => ({ ...current, tenantName: event.target.value }))
                      }
                      required
                    />
                  </label>

                  <label className="field">
                    Property name
                    <input
                      value={tenantForm.propertyName}
                      onChange={(event) =>
                        setTenantForm((current) => ({ ...current, propertyName: event.target.value }))
                      }
                      required
                    />
                  </label>

                  <label className="field">
                    Current rent amount
                    <input
                      type="number"
                      step="0.01"
                      value={tenantForm.rentAmount}
                      onChange={(event) =>
                        setTenantForm((current) => ({ ...current, rentAmount: event.target.value }))
                      }
                      required
                    />
                  </label>

                  <label className="field">
                    Due day
                    <input
                      type="number"
                      min="1"
                      max="31"
                      value={tenantForm.rentDueDay}
                      onChange={(event) =>
                        setTenantForm((current) => ({ ...current, rentDueDay: event.target.value }))
                      }
                    />
                  </label>

                  <label className="field">
                    Lease start
                    <input
                      type="date"
                      value={tenantForm.leaseStart}
                      onChange={(event) =>
                        setTenantForm((current) => ({ ...current, leaseStart: event.target.value }))
                      }
                    />
                  </label>

                  <label className="field">
                    Lease end
                    <input
                      type="date"
                      value={tenantForm.leaseEnd}
                      onChange={(event) =>
                        setTenantForm((current) => ({ ...current, leaseEnd: event.target.value }))
                      }
                    />
                  </label>

                  <label className="field">
                    Phone number
                    <input
                      value={tenantForm.phoneNumber}
                      onChange={(event) =>
                        setTenantForm((current) => ({ ...current, phoneNumber: event.target.value }))
                      }
                    />
                  </label>

                  <label className="field">
                    Unit number
                    <input
                      value={tenantForm.unitNumber}
                      onChange={(event) =>
                        setTenantForm((current) => ({ ...current, unitNumber: event.target.value }))
                      }
                    />
                  </label>

                  <label className="field field-span-2">
                    Property address
                    <input
                      value={tenantForm.propertyAddress}
                      onChange={(event) =>
                        setTenantForm((current) => ({ ...current, propertyAddress: event.target.value }))
                      }
                    />
                  </label>

                  <label className="field">
                    Security deposit
                    <input
                      type="number"
                      step="0.01"
                      value={tenantForm.securityDeposit}
                      onChange={(event) =>
                        setTenantForm((current) => ({ ...current, securityDeposit: event.target.value }))
                      }
                    />
                  </label>

                  <label className="field">
                    Emergency contact name
                    <input
                      value={tenantForm.emergencyContactName}
                      onChange={(event) =>
                        setTenantForm((current) => ({
                          ...current,
                          emergencyContactName: event.target.value,
                        }))
                      }
                    />
                  </label>

                  <label className="field">
                    Emergency contact phone
                    <input
                      value={tenantForm.emergencyContactPhone}
                      onChange={(event) =>
                        setTenantForm((current) => ({
                          ...current,
                          emergencyContactPhone: event.target.value,
                        }))
                      }
                    />
                  </label>

                  <label className="field field-span-2">
                    Lease terms
                    <textarea
                      rows="4"
                      value={tenantForm.leaseTerms}
                      onChange={(event) =>
                        setTenantForm((current) => ({ ...current, leaseTerms: event.target.value }))
                      }
                    />
                  </label>
                </div>

                <div className="rent-increase-panel">
                  <div className="section-head compact">
                    <div>
                      <p className="section-kicker">Lease Changes</p>
                      <h2>Rent increases</h2>
                    </div>
                    <button type="button" className="ghost-button" onClick={addRentIncrease} disabled={savingTenant}>
                      Add Rent Increase
                    </button>
                  </div>

                  {tenantForm.rentIncreases.length === 0 ? (
                    <div className="empty-note">No rent increases added yet.</div>
                  ) : (
                    <div className="rent-increase-list">
                      {tenantForm.rentIncreases.map((item, index) => (
                        <div key={`${item.id || "new"}-${index}`} className="rent-increase-card">
                          <div className="rent-increase-grid">
                            <label className="field">
                              Date from
                              <input
                                type="date"
                                value={item.dateFrom}
                                onChange={(event) =>
                                  updateRentIncreaseField(index, "dateFrom", event.target.value)
                                }
                                required
                              />
                            </label>

                            <label className="field">
                              Date till
                              <input
                                type="date"
                                value={item.dateTill}
                                onChange={(event) =>
                                  updateRentIncreaseField(index, "dateTill", event.target.value)
                                }
                                required
                              />
                            </label>

                            <label className="field">
                              New rent amount
                              <input
                                type="number"
                                step="0.01"
                                value={item.rentAmount}
                                onChange={(event) =>
                                  updateRentIncreaseField(index, "rentAmount", event.target.value)
                                }
                                required
                              />
                            </label>
                          </div>

                          <button
                            type="button"
                            className="ghost-danger"
                            onClick={() => removeRentIncrease(index)}
                            disabled={savingTenant}
                          >
                            Remove
                          </button>
                        </div>
                      ))}
                    </div>
                  )}
                </div>

                <button type="submit" className="primary-wide" disabled={savingTenant}>
                  {savingTenant ? "Saving..." : tenantForm.id ? "Save changes" : "Create tenant"}
                </button>
              </form>
            </section>
          )}

          {loadingPage ? (
            <div className="empty-note">Loading dashboard...</div>
          ) : properties.length === 0 ? (
            <div className="empty-note">No tenants match the current filters.</div>
          ) : (
            <div className="tenant-grid">
              {properties.map((property) => (
                <article
                  key={property.id}
                  className={`tenant-card ${selectedTenantSummary?.id === property.id ? "selected" : ""}`}
                >
                  <button
                    type="button"
                    className="tenant-card-main"
                    onClick={() => openTenantDetails(property.id)}
                  >
                    <div className="tenant-card-head">
                      <div>
                        <h3>{property.tenant_name}</h3>
                        <p className="tenant-property">{property.property_name || "Property not set"}</p>
                        <span className={`status-pill status-${property.status.toLowerCase()}`}>
                          {property.status.replaceAll("_", " ")}
                        </span>
                      </div>
                      <div className="tenant-card-aside">
                        <strong>{formatCurrency(property.balance_amount || property.surplus_amount || 0)}</strong>
                        <span>{property.balance_amount > 0 ? "Open balance" : "Current gap"}</span>
                      </div>
                    </div>

                    <div className="tenant-finance">
                      <div>
                        <p>Current rent</p>
                        <strong>{formatCurrency(property.current_rent_amount || property.rent_amount)}</strong>
                      </div>
                      <div>
                        <p>Collected</p>
                        <strong>{formatCurrency(property.current_month_paid_amount)}</strong>
                      </div>
                      <div>
                        <p>Balance</p>
                        <strong>{formatCurrency(property.balance_amount)}</strong>
                      </div>
                      <div>
                        <p>Surplus</p>
                        <strong>{formatCurrency(property.surplus_amount)}</strong>
                      </div>
                    </div>

                    <div className="tenant-meta">
                      <span>Unit: {property.unit_number || "Not set"}</span>
                      <span>Phone: {property.phone_number || "Not set"}</span>
                      <span>Due date: {formatDate(property.due_date)}</span>
                      <span>Last paid: {formatDate(property.last_paid_date)}</span>
                    </div>
                  </button>

                  <div className="tenant-actions">
                    <button
                      type="button"
                      className="ghost-button"
                      onClick={() => startEditingTenant(property)}
                      disabled={deletingTenantId === property.id}
                    >
                      Edit
                    </button>
                    <button
                      type="button"
                      className="ghost-danger"
                      onClick={() => handleDeleteTenant(property)}
                      disabled={deletingTenantId === property.id}
                    >
                      {deletingTenantId === property.id ? "Deleting..." : "Delete"}
                    </button>
                  </div>
                </article>
              ))}
            </div>
          )}
        </section>

        <section className="surface review-panel">
          <div className="section-head">
            <div>
              <p className="section-kicker">Review Inbox</p>
              <h2>Unmatched payments</h2>
            </div>
          </div>

          {unmatchedPayments.length === 0 ? (
            <div className="empty-note">No unmatched payments waiting.</div>
          ) : (
            <div className="inbox-list">
              {unmatchedPayments.map((payment) => (
                <article key={payment.id} className="inbox-item">
                  <div className="inbox-head">
                    <strong>{formatCurrency(payment.amount)}</strong>
                    <span>{formatDate(payment.payment_date)}</span>
                  </div>
                  <p>Sender: {payment.sender_key || "Unknown"}</p>
                  <p>Hint: {payment.extracted_tenant_name || "No hint"}</p>
                  {renderPaymentMatcher(payment, `unmatched-${payment.id}`)}
                </article>
              ))}
            </div>
          )}
        </section>

        {selectedTenantSummary && (
          <section className="surface detail-surface">
            <div className="section-head">
              <div>
                <p className="section-kicker">Tenant Detail</p>
                <h2>{selectedTenantSummary.tenant_name}</h2>
                <p className="tenant-property tenant-property-detail">
                  {selectedTenantSummary.property_name || "Property not set"}
                </p>
              </div>
              <button type="button" className="ghost-button" onClick={closeTenantDetails}>
                Close
              </button>
            </div>

            {detailLoading || !selectedTenantDetail ? (
              <div className="empty-note">Loading tenant details...</div>
            ) : (
              <>
                <div className="detail-hero">
                  <span className={`status-pill status-${selectedTenantDetail.property.status.toLowerCase()}`}>
                    {selectedTenantDetail.property.status.replaceAll("_", " ")}
                  </span>
                  <strong>
                    {formatCurrency(
                      selectedTenantDetail.property.current_rent_amount ||
                        selectedTenantDetail.property.rent_amount
                    )}
                  </strong>
                  <p>{formatMonthLabel(selectedMonth)} view</p>
                </div>

                <div className="detail-grid">
                  <article className="detail-card">
                    <p>Collected this month</p>
                    <strong>{formatCurrency(selectedTenantDetail.property.current_month_paid_amount)}</strong>
                  </article>
                  <article className="detail-card">
                    <p>Outstanding balance</p>
                    <strong>{formatCurrency(selectedTenantDetail.property.balance_amount)}</strong>
                  </article>
                  <article className="detail-card">
                    <p>Surplus held</p>
                    <strong>{formatCurrency(selectedTenantDetail.property.surplus_amount)}</strong>
                  </article>
                  <article className="detail-card">
                    <p>Security deposit</p>
                    <strong>{formatCurrency(selectedTenantDetail.property.security_deposit)}</strong>
                  </article>
                </div>

                <div className="detail-columns">
                  <div className="ledger-card">
                    <div className="section-head compact">
                      <div>
                        <p className="section-kicker">Profile</p>
                        <h2>Contact and lease</h2>
                      </div>
                    </div>

                    <div className="ledger-row">
                      <span>Phone</span>
                      <strong>{selectedTenantDetail.property.phone_number || "Not set"}</strong>
                    </div>
                    <div className="ledger-row">
                      <span>Unit number</span>
                      <strong>{selectedTenantDetail.property.unit_number || "Not set"}</strong>
                    </div>
                    <div className="ledger-row">
                      <span>Address</span>
                      <strong>{selectedTenantDetail.property.property_address || "Not set"}</strong>
                    </div>
                    <div className="ledger-row">
                      <span>Lease start</span>
                      <strong>{formatDate(selectedTenantDetail.property.lease_start)}</strong>
                    </div>
                    <div className="ledger-row">
                      <span>Lease end</span>
                      <strong>{formatDate(selectedTenantDetail.property.lease_end)}</strong>
                    </div>
                    <div className="ledger-row">
                      <span>Emergency contact</span>
                      <strong>
                        {selectedTenantDetail.property.emergency_contact_name || "Not set"}
                        {selectedTenantDetail.property.emergency_contact_phone
                          ? ` • ${selectedTenantDetail.property.emergency_contact_phone}`
                          : ""}
                      </strong>
                    </div>
                    <div className="ledger-stack">
                      <span>Lease terms</span>
                      <strong>{selectedTenantDetail.property.lease_terms || "Not set"}</strong>
                    </div>
                  </div>

                  <div className="ledger-card">
                    <div className="section-head compact">
                      <div>
                        <p className="section-kicker">Reminders</p>
                        <h2>Tenant-specific alerts</h2>
                      </div>
                    </div>

                    {selectedTenantDetail.reminders.length ? (
                      <div className="reminder-list">
                        {selectedTenantDetail.reminders.map((item, index) => (
                          <article
                            key={`${item.type}-${index}`}
                            className={`reminder-card ${severityClass(item.severity)}`}
                          >
                            <strong>{item.title}</strong>
                            <p>{item.description}</p>
                          </article>
                        ))}
                      </div>
                    ) : (
                      <div className="empty-note">No active alerts for this tenant.</div>
                    )}
                  </div>
                </div>

                <div className="detail-columns">
                  <div className="ledger-card">
                    <div className="section-head compact">
                      <div>
                        <p className="section-kicker">Ledger</p>
                        <h2>Payment history</h2>
                      </div>
                    </div>

                    <div className="history-filter-bar">
                      <label className="field">
                        From
                        <input
                          type="date"
                          value={tenantHistoryFilter.from}
                          onChange={(event) =>
                            setTenantHistoryFilter((current) => ({
                              ...current,
                              from: event.target.value,
                            }))
                          }
                        />
                      </label>

                      <label className="field">
                        To
                        <input
                          type="date"
                          value={tenantHistoryFilter.to}
                          onChange={(event) =>
                            setTenantHistoryFilter((current) => ({
                              ...current,
                              to: event.target.value,
                            }))
                          }
                        />
                      </label>

                      <button
                        type="button"
                        className="ghost-button"
                        onClick={() => setTenantHistoryFilter(emptyHistoryFilter)}
                      >
                        Clear Filter
                      </button>
                    </div>

                    <div className="history-summary-grid">
                      <article className="detail-card compact-detail-card">
                        <p>Collected in range</p>
                        <strong>
                          {formatCurrency(selectedTenantDetail.payment_history_summary.collected_total)}
                        </strong>
                      </article>
                      <article className="detail-card compact-detail-card">
                        <p>Posted payments</p>
                        <strong>{selectedTenantDetail.payment_history_summary.posted_count}</strong>
                      </article>
                      <article className="detail-card compact-detail-card">
                        <p>Last collected on</p>
                        <strong>{formatDate(selectedTenantDetail.payment_history_summary.last_collected_on)}</strong>
                      </article>
                    </div>

                    {selectedTenantDetail.payment_history.length ? (
                      <div className="history-list">
                        {selectedTenantDetail.payment_history.map((payment) => (
                          <div key={payment.id} className="history-row">
                            <div>
                              <strong>{formatCurrency(payment.amount)}</strong>
                              <p>
                                {formatDate(payment.payment_date)} • {payment.source.replaceAll("_", " ")}
                              </p>
                              <span className={`status-pill status-${payment.status.toLowerCase()}`}>
                                {payment.status}
                              </span>
                            </div>
                            <button
                              type="button"
                              className="ghost-danger"
                              onClick={() => handleUndoPayment(payment.id)}
                              disabled={payment.status !== "POSTED" || busyUndoIds.includes(payment.id)}
                            >
                              {busyUndoIds.includes(payment.id) ? "Undoing..." : "Undo"}
                            </button>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="empty-note">No payments recorded yet.</div>
                    )}
                  </div>

                  <div className="ledger-card">
                    <div className="section-head compact">
                      <div>
                        <p className="section-kicker">Monthly View</p>
                        <h2>Recent months</h2>
                      </div>
                    </div>

                    <div className="history-list">
                      {selectedTenantDetail.monthly_history.map((item) => (
                        <div key={item.month} className="history-row history-row-stacked">
                          <div>
                            <strong>{formatMonthLabel(item.month)}</strong>
                            <p>
                              Expected {formatCurrency(item.expected)} • Collected {formatCurrency(item.collected)}
                            </p>
                            <p>
                              Balance {formatCurrency(item.balance)} • Surplus {formatCurrency(item.surplus)}
                            </p>
                          </div>
                          <span className={`status-pill status-${item.status.toLowerCase()}`}>
                            {item.status.replaceAll("_", " ")}
                          </span>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>

                <div className="detail-columns">
                  <div className="rent-history-card">
                    <div className="section-head compact">
                      <div>
                        <p className="section-kicker">Lease Changes</p>
                        <h2>Rent increase history</h2>
                      </div>
                    </div>

                    {selectedTenantDetail.property.rent_increases?.length ? (
                      <div className="rent-history-list">
                        {selectedTenantDetail.property.rent_increases.map((item) => (
                          <div key={item.id || `${item.date_from}-${item.date_till}`} className="ledger-row">
                            <span>
                              {formatDate(item.date_from)} to {formatDate(item.date_till)}
                            </span>
                            <strong>{formatCurrency(item.rent_amount)}</strong>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="empty-note">No rent increases recorded for this tenant.</div>
                    )}
                  </div>

                  <div className="ledger-card">
                    <div className="section-head compact">
                      <div>
                        <p className="section-kicker">Quick Actions</p>
                        <h2>Manage this tenant</h2>
                      </div>
                    </div>

                    <div className="detail-actions">
                      <button
                        type="button"
                        className="ghost-button"
                        onClick={() => startEditingTenant(selectedTenantDetail.property)}
                      >
                        Edit Tenant
                      </button>
                      <button
                        type="button"
                        className="ghost-danger"
                        onClick={() => handleDeleteTenant(selectedTenantDetail.property)}
                        disabled={deletingTenantId === selectedTenantDetail.property.id}
                      >
                        {deletingTenantId === selectedTenantDetail.property.id ? "Deleting..." : "Delete"}
                      </button>
                    </div>
                  </div>
                </div>
              </>
            )}
          </section>
        )}
      </main>
    </div>
  );
}

export default App;
