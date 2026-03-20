import React, { useEffect, useState } from "react";
import "./styles.css";

const API_BASE = process.env.REACT_APP_API_BASE || "http://localhost:8000";

const emptyUploadForm = {
  tenantName: "",
  rentAmount: "",
  file: null,
};

const emptyManualTenantForm = {
  tenantName: "",
  rentAmount: "",
  rentDueDay: "",
  leaseStart: "",
  leaseEnd: "",
};

function App() {
  const [properties, setProperties] = useState([]);
  const [unmatchedPayments, setUnmatchedPayments] = useState([]);
  const [uploadForm, setUploadForm] = useState(emptyUploadForm);
  const [manualTenantForm, setManualTenantForm] = useState(emptyManualTenantForm);
  const [uploadResult, setUploadResult] = useState(null);
  const [paymentMessage, setPaymentMessage] = useState("");
  const [paymentResult, setPaymentResult] = useState(null);
  const [manualSelections, setManualSelections] = useState({});
  const [error, setError] = useState("");
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

  const handleUpload = async (event) => {
    event.preventDefault();
    if (!uploadForm.file) {
      setError("Please choose a PDF lease file.");
      return;
    }

    setLoading(true);
    setError("");
    setUploadResult(null);

    try {
      const formData = new FormData();
      formData.append("file", uploadForm.file);
      formData.append("tenant_name", uploadForm.tenantName);
      formData.append("rent_amount", uploadForm.rentAmount);

      const response = await fetch(`${API_BASE}/upload-lease`, {
        method: "POST",
        body: formData,
      });

      const result = await response.json();
      if (!response.ok) {
        throw new Error(result.detail || "Lease upload failed.");
      }

      setUploadResult(result);
      setUploadForm(emptyUploadForm);
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

  const handleManualTenantCreate = async (event) => {
    event.preventDefault();
    setLoading(true);
    setError("");
    setUploadResult(null);

    try {
      const response = await fetch(`${API_BASE}/properties`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          tenant_name: manualTenantForm.tenantName,
          rent_amount: Number(manualTenantForm.rentAmount),
          rent_due_day: manualTenantForm.rentDueDay ? Number(manualTenantForm.rentDueDay) : null,
          lease_start: manualTenantForm.leaseStart || null,
          lease_end: manualTenantForm.leaseEnd || null,
        }),
      });
      const result = await response.json();
      if (!response.ok) {
        throw new Error(result.detail || "Could not add tenant.");
      }

      setUploadResult(result);
      setManualTenantForm(emptyManualTenantForm);
      await loadData();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleManualMatch = async (unmatchedPayment) => {
    const propertyId = manualSelections[unmatchedPayment.id];
    if (!propertyId) {
      setError("Please select a property for manual matching.");
      return;
    }

    setLoading(true);
    setError("");

    try {
      const response = await fetch(`${API_BASE}/manual-match`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          property_id: Number(propertyId),
          amount: unmatchedPayment.amount,
          date: unmatchedPayment.payment_date,
          unmatched_payment_id: unmatchedPayment.id,
          sender_key: unmatchedPayment.sender_key || null,
        }),
      });
      const result = await response.json();
      if (!response.ok) {
        throw new Error(result.detail || "Manual match failed.");
      }

      setPaymentResult(result);
      await loadData();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const paymentResultSelectionKey =
    paymentResult?.status === "UNMATCHED" ? `payment-result-${paymentResult.unmatched_payment_id}` : null;

  const handlePaymentResultMatch = async () => {
    if (!paymentResultSelectionKey || paymentResult?.status !== "UNMATCHED") {
      return;
    }

    const propertyId = manualSelections[paymentResultSelectionKey];
    if (!propertyId) {
      setError("Please select a tenant to match this payment.");
      return;
    }

    setLoading(true);
    setError("");

    try {
      const response = await fetch(`${API_BASE}/manual-match`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          property_id: Number(propertyId),
          amount: paymentResult.amount,
          date: paymentResult.date,
          unmatched_payment_id: paymentResult.unmatched_payment_id,
          sender_key: paymentResult.sender_key || null,
        }),
      });
      const result = await response.json();
      if (!response.ok) {
        throw new Error(result.detail || "Manual match failed.");
      }

      setPaymentResult(result);
      await loadData();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="app-shell">
      <header className="hero">
        <div>
          <p className="eyebrow">Rent Management System</p>
          <h1>Lease parsing, rent tracking, and payment correction.</h1>
          <p className="hero-copy">
            Upload leases, track rent status, process bank messages, and manually
            resolve unmatched payments.
          </p>
        </div>
      </header>

      {error && <div className="alert error">{error}</div>}

      <main className="grid">
        <section className="card">
          <h2>Upload Lease</h2>
          <form onSubmit={handleUpload} className="stack">
            <label>
              Tenant Name
              <input
                value={uploadForm.tenantName}
                onChange={(e) =>
                  setUploadForm((current) => ({
                    ...current,
                    tenantName: e.target.value,
                  }))
                }
                required
              />
            </label>
            <label>
              Rent Amount
              <input
                type="number"
                step="0.01"
                value={uploadForm.rentAmount}
                onChange={(e) =>
                  setUploadForm((current) => ({
                    ...current,
                    rentAmount: e.target.value,
                  }))
                }
                required
              />
            </label>
            <label>
              Lease PDF
              <input
                type="file"
                accept="application/pdf"
                onChange={(e) =>
                  setUploadForm((current) => ({
                    ...current,
                    file: e.target.files[0] || null,
                  }))
                }
                required
              />
            </label>
            <button type="submit" disabled={loading}>
              {loading ? "Working..." : "Upload Lease"}
            </button>
          </form>

          {uploadResult && (
            <div className="result">
              <h3>Extracted Data</h3>
              <pre>{JSON.stringify(uploadResult, null, 2)}</pre>
            </div>
          )}
        </section>

        <section className="card">
          <h2>Add Tenant Without Lease</h2>
          <form onSubmit={handleManualTenantCreate} className="stack">
            <label>
              Tenant Name
              <input
                value={manualTenantForm.tenantName}
                onChange={(e) =>
                  setManualTenantForm((current) => ({
                    ...current,
                    tenantName: e.target.value,
                  }))
                }
                required
              />
            </label>
            <label>
              Amount Due
              <input
                type="number"
                step="0.01"
                value={manualTenantForm.rentAmount}
                onChange={(e) =>
                  setManualTenantForm((current) => ({
                    ...current,
                    rentAmount: e.target.value,
                  }))
                }
                required
              />
            </label>
            <label>
              Due Day (Optional)
              <input
                type="number"
                min="1"
                max="31"
                value={manualTenantForm.rentDueDay}
                onChange={(e) =>
                  setManualTenantForm((current) => ({
                    ...current,
                    rentDueDay: e.target.value,
                  }))
                }
              />
            </label>
            <label>
              Lease Start (Optional)
              <input
                type="date"
                value={manualTenantForm.leaseStart}
                onChange={(e) =>
                  setManualTenantForm((current) => ({
                    ...current,
                    leaseStart: e.target.value,
                  }))
                }
              />
            </label>
            <label>
              Lease End (Optional)
              <input
                type="date"
                value={manualTenantForm.leaseEnd}
                onChange={(e) =>
                  setManualTenantForm((current) => ({
                    ...current,
                    leaseEnd: e.target.value,
                  }))
                }
              />
            </label>
            <button type="submit" disabled={loading}>
              {loading ? "Working..." : "Add Tenant"}
            </button>
          </form>
        </section>

        <section className="card wide">
          <h2>Dashboard</h2>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Tenant Name</th>
                  <th>Rent Amount</th>
                  <th>Paid This Month</th>
                  <th>Balance</th>
                  <th>Due Day</th>
                  <th>Lease Start</th>
                  <th>Lease End</th>
                  <th>Status</th>
                  <th>Last Paid Date</th>
                </tr>
              </thead>
              <tbody>
                {properties.length === 0 ? (
                  <tr>
                    <td colSpan="9">No properties yet.</td>
                  </tr>
                ) : (
                  properties.map((property) => (
                    <tr key={property.id}>
                      <td>{property.tenant_name}</td>
                      <td>{property.rent_amount}</td>
                      <td>{property.current_month_paid_amount || 0}</td>
                      <td>{property.balance_amount ?? property.rent_amount}</td>
                      <td>{property.rent_due_day || "-"}</td>
                      <td>{property.lease_start || "-"}</td>
                      <td>{property.lease_end || "-"}</td>
                      <td>
                        <span className={`status ${property.status.toLowerCase()}`}>
                          {property.status}
                        </span>
                      </td>
                      <td>{property.last_paid_date || "-"}</td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </section>

        <section className="card">
          <h2>Process Payment</h2>
          <label className="stack">
            Bank Message
            <textarea
              rows="5"
              value={paymentMessage}
              onChange={(e) => setPaymentMessage(e.target.value)}
              placeholder='ICICI Bank Account XX112 credited:Rs. 1,80,374.64 on 12-Mar-26. Info NEFT-HSBCN07127765530-RISHI'
            />
          </label>
          <button onClick={handlePaymentProcess} disabled={loading || !paymentMessage.trim()}>
            {loading ? "Working..." : "Process Payment"}
          </button>

          {paymentResult && (
            <div className="result">
              <h3>Payment Result</h3>
              {paymentResult.matched_property && (
                <p className="helper-text">
                  Status: <strong>{paymentResult.status}</strong>
                  {" | "}Paid this month:{" "}
                  <strong>{paymentResult.current_month_paid_amount ?? 0}</strong>
                  {" | "}Balance: <strong>{paymentResult.balance_amount ?? 0}</strong>
                </p>
              )}
              <pre>{JSON.stringify(paymentResult, null, 2)}</pre>
              {paymentResult.status === "UNMATCHED" && (
                <div className="stack result-matcher">
                  <p className="helper-text">
                    New sender key <strong>{paymentResult.sender_key || "unknown"}</strong>.
                    Match it once and future payments from the same NEFT sender will auto-map.
                  </p>
                  <select
                    value={manualSelections[paymentResultSelectionKey] || ""}
                    onChange={(e) =>
                      setManualSelections((current) => ({
                        ...current,
                        [paymentResultSelectionKey]: e.target.value,
                      }))
                    }
                  >
                    <option value="">Select tenant</option>
                    {(paymentResult.candidates.length > 0
                      ? paymentResult.candidates
                      : properties
                    ).map((property) => (
                      <option key={property.id} value={property.id}>
                        {property.tenant_name} (#{property.id})
                      </option>
                    ))}
                  </select>
                  <button onClick={handlePaymentResultMatch} disabled={loading}>
                    Save Match For This Sender
                  </button>
                </div>
              )}
            </div>
          )}
        </section>

        <section className="card wide">
          <h2>Manual Matching</h2>
          {unmatchedPayments.length === 0 ? (
            <p>No unmatched payments.</p>
          ) : (
            <div className="stack">
              {unmatchedPayments.map((payment) => (
                <div key={payment.id} className="unmatched-card">
                  <div className="unmatched-meta">
                    <p>
                      <strong>Tenant Key:</strong> {payment.extracted_tenant_name || "N/A"}
                    </p>
                    <p>
                      <strong>Sender Key:</strong> {payment.sender_key || "N/A"}
                    </p>
                    <p>
                      <strong>Amount:</strong> {payment.amount}
                    </p>
                    <p>
                      <strong>Date:</strong> {payment.payment_date}
                    </p>
                  </div>
                  <select
                    value={manualSelections[payment.id] || ""}
                    onChange={(e) =>
                      setManualSelections((current) => ({
                        ...current,
                        [payment.id]: e.target.value,
                      }))
                    }
                  >
                    <option value="">Select property</option>
                    {(payment.candidates.length > 0 ? payment.candidates : properties).map(
                      (property) => (
                        <option key={property.id} value={property.id}>
                          {property.tenant_name} (#{property.id})
                        </option>
                      )
                    )}
                  </select>
                  <button onClick={() => handleManualMatch(payment)} disabled={loading}>
                    Confirm Match
                  </button>
                </div>
              ))}
            </div>
          )}
        </section>
      </main>
    </div>
  );
}

export default App;
