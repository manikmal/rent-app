import React, { useEffect, useMemo, useState } from "react";
import "./styles.css";

const API_BASE = process.env.REACT_APP_API_BASE || "http://localhost:8000";

const emptyTenantForm = {
  id: null,
  tenantName: "",
  rentAmount: "",
  rentDueDay: "",
  leaseStart: "",
  leaseEnd: "",
};

function toTenantForm(property) {
  return {
    id: property?.id ?? null,
    tenantName: property?.tenant_name ?? "",
    rentAmount: property?.rent_amount ?? "",
    rentDueDay: property?.rent_due_day ?? "",
    leaseStart: property?.lease_start ?? "",
    leaseEnd: property?.lease_end ?? "",
  };
}

function formatCurrency(value) {
  return `Rs. ${Number(value || 0).toLocaleString("en-IN", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

function metricRows(properties, unmatchedPayments) {
  const totalExpected = properties.reduce((sum, item) => sum + Number(item.rent_amount || 0), 0);
  const totalCollected = properties.reduce(
    (sum, item) => sum + Number(item.current_month_paid_amount || 0),
    0
  );
  const totalOutstanding = properties.reduce(
    (sum, item) => sum + Number(item.balance_amount || 0),
    0
  );
  const totalSurplus = properties.reduce(
    (sum, item) => sum + Number(item.surplus_amount || 0),
    0
  );
  const needsAttention = properties.filter((item) =>
    ["LATE", "PARTIALLY_PAID", "SURPLUS"].includes(item.status)
  ).length;

  return [
    { label: "Expected this month", value: formatCurrency(totalExpected), tone: "sand" },
    { label: "Collected", value: formatCurrency(totalCollected), tone: "mint" },
    { label: "Outstanding", value: formatCurrency(totalOutstanding), tone: "amber" },
    { label: "Surplus", value: formatCurrency(totalSurplus), tone: "blue" },
    { label: "Unmatched payments", value: String(unmatchedPayments.length), tone: "coral" },
    { label: "Needs attention", value: String(needsAttention), tone: "ink" },
  ];
}

function App() {
  const [properties, setProperties] = useState([]);
  const [unmatchedPayments, setUnmatchedPayments] = useState([]);
  const [selectedTenant, setSelectedTenant] = useState(null);
  const [tenantForm, setTenantForm] = useState(emptyTenantForm);
  const [showTenantForm, setShowTenantForm] = useState(false);
  const [paymentMessage, setPaymentMessage] = useState("");
  const [paymentResult, setPaymentResult] = useState(null);
  const [manualSelections, setManualSelections] = useState({});
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [loading, setLoading] = useState(false);

  const loadData = async () => {
    try {
      const [propertiesRes, unmatchedRes] = await Promise.all([
        fetch(`${API_BASE}/properties`),
        fetch(`${API_BASE}/unmatched-payments`),
      ]);

      if (!propertiesRes.ok || !unmatchedRes.ok) {
        throw new Error("Failed to load dashboard data.");
      }

      const propertiesJson = await propertiesRes.json();
      const unmatchedJson = await unmatchedRes.json();
      setProperties(propertiesJson);
      setUnmatchedPayments(unmatchedJson);
    } catch (err) {
      setError(err.message);
    }
  };

  useEffect(() => {
    loadData();
  }, []);

  const metrics = useMemo(
    () => metricRows(properties, unmatchedPayments),
    [properties, unmatchedPayments]
  );

  const attentionItems = useMemo(
    () =>
      properties.filter((item) => ["LATE", "PARTIALLY_PAID", "SURPLUS"].includes(item.status)),
    [properties]
  );

  const resetTenantEditor = () => {
    setTenantForm(emptyTenantForm);
    setShowTenantForm(false);
  };

  const openTenantDetails = (property) => {
    setSelectedTenant(property);
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

  const handleTenantSave = async (event) => {
    event.preventDefault();
    setLoading(true);
    setError("");
    setNotice("");

    try {
      const payload = {
        tenant_name: tenantForm.tenantName,
        rent_amount: Number(tenantForm.rentAmount),
        rent_due_day: tenantForm.rentDueDay ? Number(tenantForm.rentDueDay) : null,
        lease_start: tenantForm.leaseStart || null,
        lease_end: tenantForm.leaseEnd || null,
      };

      const isEditing = Boolean(tenantForm.id);
      const response = await fetch(
        isEditing ? `${API_BASE}/properties/${tenantForm.id}` : `${API_BASE}/properties`,
        {
          method: isEditing ? "PUT" : "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        }
      );
      const result = await response.json();
      if (!response.ok) {
        throw new Error(result.detail || "Could not save tenant.");
      }

      setNotice(isEditing ? "Tenant updated." : "Tenant added.");
      resetTenantEditor();
      await loadData();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteTenant = async (property) => {
    if (!window.confirm(`Delete ${property.tenant_name}?`)) {
      return;
    }

    setLoading(true);
    setError("");
    setNotice("");

    try {
      const response = await fetch(`${API_BASE}/properties/${property.id}`, {
        method: "DELETE",
      });
      const result = await response.json();
      if (!response.ok) {
        throw new Error(result.detail || "Could not delete tenant.");
      }

      if (tenantForm.id === property.id) {
        resetTenantEditor();
      }
      if (selectedTenant?.id === property.id) {
        setSelectedTenant(null);
      }
      setNotice("Tenant deleted.");
      await loadData();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handlePaymentProcess = async () => {
    setLoading(true);
    setError("");
    setNotice("");
    setPaymentResult(null);

    try {
      const response = await fetch(`${API_BASE}/process-payment`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: paymentMessage }),
      });
      const result = await response.json();
      if (!response.ok) {
        throw new Error(result.detail || "Payment processing failed.");
      }

      setPaymentResult(result);
      setPaymentMessage("");
      await loadData();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const savePaymentMatch = async ({ selectionKey, propertyId, amount, date, unmatchedPaymentId, senderKey }) => {
    if (!propertyId) {
      setError("Please select a tenant to match this payment.");
      return;
    }

    setLoading(true);
    setError("");
    setNotice("");

    try {
      const response = await fetch(`${API_BASE}/manual-match`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          property_id: Number(propertyId),
          amount,
          date,
          unmatched_payment_id: unmatchedPaymentId,
          sender_key: senderKey || null,
        }),
      });
      const result = await response.json();
      if (!response.ok) {
        throw new Error(result.detail || "Manual match failed.");
      }

      setManualSelections((current) => ({
        ...current,
        [selectionKey]: "",
      }));
      setPaymentResult(result);
      setNotice("Payment matched.");
      await loadData();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const renderPaymentMatcher = (payment, selectionKey) => (
    <div className="matcher-inline">
      <select
        value={manualSelections[selectionKey] || ""}
        onChange={(e) =>
          setManualSelections((current) => ({
            ...current,
            [selectionKey]: e.target.value,
          }))
        }
      >
        <option value="">Choose tenant</option>
        {(payment.candidates?.length > 0 ? payment.candidates : properties).map((property) => (
          <option key={property.id} value={property.id}>
            {property.tenant_name} • {formatCurrency(property.rent_amount)}
          </option>
        ))}
      </select>
      <button
        type="button"
        onClick={() =>
          savePaymentMatch({
            selectionKey,
            propertyId: manualSelections[selectionKey],
            amount: payment.amount,
            date: payment.date || payment.payment_date,
            unmatchedPaymentId: payment.unmatched_payment_id || payment.id,
            senderKey: payment.sender_key,
          })
        }
        disabled={loading}
      >
        Match
      </button>
    </div>
  );

  const selectedTenantFresh =
    selectedTenant && properties.find((property) => property.id === selectedTenant.id);

  return (
    <div className="app-shell">
      <header className="hero">
        <div className="hero-copy-wrap">
          <p className="eyebrow">Rent Desk</p>
          <h1>Rent collection first. Everything else stays out of the way.</h1>
          <p className="hero-copy">
            Track what is due, what has landed, who needs attention, and match incoming
            payments without jumping between forms.
          </p>
        </div>
        <div className="hero-cta">
          <button type="button" onClick={openNewTenantForm} disabled={loading}>
            Add Tenant
          </button>
        </div>
      </header>

      {error && <div className="banner error">{error}</div>}
      {notice && <div className="banner success">{notice}</div>}

      <section className="metrics-grid">
        {metrics.map((metric) => (
          <article key={metric.label} className={`metric-card tone-${metric.tone}`}>
            <p>{metric.label}</p>
            <strong>{metric.value}</strong>
          </article>
        ))}
      </section>

      <main className="dashboard-grid">
        {showTenantForm && (
          <section className="surface editor-surface editor-surface-inline">
            <div className="section-head">
              <div>
                <p className="section-kicker">Editor</p>
                <h2>{tenantForm.id ? "Edit tenant" : "Add tenant"}</h2>
              </div>
              <button type="button" className="ghost-button" onClick={resetTenantEditor} disabled={loading}>
                Close
              </button>
            </div>

            <form onSubmit={handleTenantSave} className="editor-form">
              <label className="field">
                Tenant name
                <input
                  value={tenantForm.tenantName}
                  onChange={(e) =>
                    setTenantForm((current) => ({
                      ...current,
                      tenantName: e.target.value,
                    }))
                  }
                  required
                />
              </label>

              <label className="field">
                Rent amount
                <input
                  type="number"
                  step="0.01"
                  value={tenantForm.rentAmount}
                  onChange={(e) =>
                    setTenantForm((current) => ({
                      ...current,
                      rentAmount: e.target.value,
                    }))
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
                  onChange={(e) =>
                    setTenantForm((current) => ({
                      ...current,
                      rentDueDay: e.target.value,
                    }))
                  }
                />
              </label>

              <label className="field">
                Lease start
                <input
                  type="date"
                  value={tenantForm.leaseStart}
                  onChange={(e) =>
                    setTenantForm((current) => ({
                      ...current,
                      leaseStart: e.target.value,
                    }))
                  }
                />
              </label>

              <label className="field">
                Lease end
                <input
                  type="date"
                  value={tenantForm.leaseEnd}
                  onChange={(e) =>
                    setTenantForm((current) => ({
                      ...current,
                      leaseEnd: e.target.value,
                    }))
                  }
                />
              </label>

              <button type="submit" className="primary-wide" disabled={loading}>
                {loading ? "Saving..." : tenantForm.id ? "Save changes" : "Create tenant"}
              </button>
            </form>
          </section>
        )}

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
              onChange={(e) => setPaymentMessage(e.target.value)}
              placeholder="Paste the credited NEFT message here"
            />
          </label>

          <button
            type="button"
            className="primary-wide"
            onClick={handlePaymentProcess}
            disabled={loading || !paymentMessage.trim()}
          >
            {loading ? "Working..." : "Process Payment"}
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
                  <span>Paid this month: {formatCurrency(paymentResult.current_month_paid_amount)}</span>
                  <span>Balance: {formatCurrency(paymentResult.balance_amount)}</span>
                  <span>Surplus: {formatCurrency(paymentResult.surplus_amount)}</span>
                </div>
              ) : (
                <div className="payment-summary">
                  <p>Manual match needed</p>
                  <span>
                    {paymentResult.matching_hint || "We could not confidently identify this payment."}
                  </span>
                </div>
              )}

              {paymentResult.status === "UNMATCHED" &&
                renderPaymentMatcher(paymentResult, `payment-result-${paymentResult.unmatched_payment_id}`)}
            </div>
          ) : (
            <div className="empty-note">
              Payments you process will show a match result, balance, and surplus here.
            </div>
          )}
        </section>

        <section className="surface sidebar-panel">
          <div className="section-head">
            <div>
              <p className="section-kicker">Attention</p>
              <h2>What needs action</h2>
            </div>
          </div>

          {attentionItems.length === 0 ? (
            <div className="empty-note">Everything looks settled right now.</div>
          ) : (
            <div className="attention-list">
              {attentionItems.map((item) => (
                <button
                  key={item.id}
                  type="button"
                  className="attention-item"
                  onClick={() => openTenantDetails(item)}
                >
                  <div>
                    <strong>{item.tenant_name}</strong>
                    <span>{item.status.replaceAll("_", " ")}</span>
                  </div>
                  <small>{formatCurrency(item.balance_amount || item.surplus_amount || 0)}</small>
                </button>
              ))}
            </div>
          )}

          <div className="section-head compact">
            <div>
              <p className="section-kicker">Unmatched</p>
              <h2>Payments inbox</h2>
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
                    <span>{payment.payment_date}</span>
                  </div>
                  <p>Sender: {payment.sender_key || "Unknown"}</p>
                  <p>Hint: {payment.extracted_tenant_name || "No hint"}</p>
                  {renderPaymentMatcher(payment, `unmatched-${payment.id}`)}
                </article>
              ))}
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

          {properties.length === 0 ? (
            <div className="empty-note">No tenants yet. Start by adding one.</div>
          ) : (
            <div className="tenant-grid">
              {properties.map((property) => (
                <article
                  key={property.id}
                  className={`tenant-card ${selectedTenantFresh?.id === property.id ? "selected" : ""}`}
                  onClick={() => openTenantDetails(property)}
                >
                  <div className="tenant-card-head">
                    <div>
                      <h3>{property.tenant_name}</h3>
                      <span className={`status-pill status-${property.status.toLowerCase()}`}>
                        {property.status.replaceAll("_", " ")}
                      </span>
                    </div>
                    <div className="tenant-actions">
                      <button
                        type="button"
                        className="ghost-button"
                        onClick={(event) => {
                          event.stopPropagation();
                          startEditingTenant(property);
                        }}
                        disabled={loading}
                      >
                        Edit
                      </button>
                      <button
                        type="button"
                        className="ghost-danger"
                        onClick={(event) => {
                          event.stopPropagation();
                          handleDeleteTenant(property);
                        }}
                        disabled={loading}
                      >
                        Delete
                      </button>
                    </div>
                  </div>

                  <div className="tenant-finance">
                    <div>
                      <p>Monthly rent</p>
                      <strong>{formatCurrency(property.rent_amount)}</strong>
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
                    <span>Due day: {property.rent_due_day || "Not set"}</span>
                    <span>Lease start: {property.lease_start || "Not set"}</span>
                    <span>Lease end: {property.lease_end || "Not set"}</span>
                    <span>Last paid: {property.last_paid_date || "No payment yet"}</span>
                  </div>
                </article>
              ))}
            </div>
          )}
        </section>

        {selectedTenantFresh && (
          <section className="surface detail-surface">
            <div className="section-head">
              <div>
                <p className="section-kicker">Tenant Detail</p>
                <h2>{selectedTenantFresh.tenant_name}</h2>
              </div>
              <button
                type="button"
                className="ghost-button"
                onClick={() => setSelectedTenant(null)}
                disabled={loading}
              >
                Close
              </button>
            </div>

            <div className="detail-hero">
              <span className={`status-pill status-${selectedTenantFresh.status.toLowerCase()}`}>
                {selectedTenantFresh.status.replaceAll("_", " ")}
              </span>
              <strong>{formatCurrency(selectedTenantFresh.rent_amount)}</strong>
              <p>Monthly rent</p>
            </div>

            <div className="detail-grid">
              <article className="detail-card">
                <p>Collected this month</p>
                <strong>{formatCurrency(selectedTenantFresh.current_month_paid_amount)}</strong>
              </article>
              <article className="detail-card">
                <p>Outstanding balance</p>
                <strong>{formatCurrency(selectedTenantFresh.balance_amount)}</strong>
              </article>
              <article className="detail-card">
                <p>Surplus held</p>
                <strong>{formatCurrency(selectedTenantFresh.surplus_amount)}</strong>
              </article>
              <article className="detail-card">
                <p>Due day</p>
                <strong>{selectedTenantFresh.rent_due_day || "Not set"}</strong>
              </article>
            </div>

            <div className="ledger-card">
              <div className="ledger-row">
                <span>Lease start</span>
                <strong>{selectedTenantFresh.lease_start || "Not set"}</strong>
              </div>
              <div className="ledger-row">
                <span>Lease end</span>
                <strong>{selectedTenantFresh.lease_end || "Not set"}</strong>
              </div>
              <div className="ledger-row">
                <span>Last paid date</span>
                <strong>{selectedTenantFresh.last_paid_date || "No payment yet"}</strong>
              </div>
              <div className="ledger-row">
                <span>Last payment amount</span>
                <strong>{formatCurrency(selectedTenantFresh.last_payment_amount)}</strong>
              </div>
            </div>

            <div className="detail-actions">
              <button
                type="button"
                className="ghost-button"
                onClick={() => startEditingTenant(selectedTenantFresh)}
                disabled={loading}
              >
                Edit Tenant
              </button>
              <button
                type="button"
                className="ghost-danger"
                onClick={() => handleDeleteTenant(selectedTenantFresh)}
                disabled={loading}
              >
                Delete Tenant
              </button>
            </div>
          </section>
        )}

      </main>
    </div>
  );
}

export default App;
