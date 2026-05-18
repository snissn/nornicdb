# HIPAA Compliance

**How NornicDB's security features satisfy HIPAA requirements.**

Last Updated: April 2026

---

## Overview

NornicDB encrypts and protects all data in the database uniformly — not just specific fields or record types. This all-or-nothing approach simplifies HIPAA compliance because there is no need to identify, tag, or configure individual Protected Health Information (PHI) fields. Every node, edge, index, and metadata entry receives the same encryption, access control, and audit treatment.

This document maps NornicDB's built-in security features to the HIPAA Security Rule and provides a configuration guide for covered entities and business associates.

---

## HIPAA Security Rule Mapping

### Administrative Safeguards (§164.308)

| Requirement | Section | NornicDB Feature |
|---|---|---|
| Security Management | (a)(1) | Audit logging, security alerting |
| Workforce Security | (a)(3) | RBAC, user management |
| Information Access | (a)(4) | Role-based permissions, per-database access control |
| Security Training | (a)(5) | Audit trails for access review |
| Security Incidents | (a)(6) | Security event alerting |
| Contingency Plan | (a)(7) | Backup, restore, WAL recovery |

### Technical Safeguards (§164.312)

| Requirement | Section | NornicDB Feature |
|---|---|---|
| Access Control | (a)(1) | JWT authentication, RBAC, per-database roles |
| Audit Controls | (b) | Comprehensive audit logging with retention |
| Integrity | (c)(1) | AES-256-GCM encryption, hash-chained audit logs |
| Person Authentication | (d) | Unique user IDs, password policies |
| Transmission Security | (e)(1) | TLS 1.2+ (TLS 1.3 preferred) |

### Physical Safeguards (§164.310)

| Requirement | Section | Responsibility |
|---|---|---|
| Facility Access | (a)(1) | Your infrastructure / cloud provider |
| Workstation Security | (b) | Your organization |
| Device Controls | (d)(1) | Your organization |

---

## Encryption at Rest

NornicDB uses **full database encryption** at the storage layer. When enabled, all data is encrypted — every node, edge, property, index entry, and internal metadata. There is no per-field configuration because everything is protected equally.

```yaml
database:
  encryption_enabled: true
  encryption_password: "your-secure-password-here"
```

- **Algorithm**: AES-256-GCM with PBKDF2 key derivation
- **Scope**: All data, indexes, and metadata
- **No field-level configuration needed** — encryption is all-or-nothing
- **Important**: If you lose your encryption password, your data cannot be recovered. Store it securely using a secrets manager (e.g. HashiCorp Vault, GCP Secret Manager).

---

## Access Control (§164.312(a))

### Authentication

Every user has a unique identity. NornicDB supports JWT-based authentication with configurable session policies:

```yaml
auth:
  enabled: true
  session_timeout: 15m
  max_failed_attempts: 3
  lockout_duration: 30m
```

### Role-Based Access Control

Define roles that enforce minimum-necessary access. Permissions control what each role can read, write, and administer:

```yaml
rbac:
  enabled: true
  default_role: none  # No access by default
  roles:
    - name: clinician
      permissions: [read, write]
      databases: [patient_records]
    - name: admin
      permissions: [read, write, manage_users, manage_databases]
    - name: analyst
      permissions: [read]
      databases: [analytics]
```

Use per-database access control to restrict roles to specific databases, ensuring users only access the data they need.

### Minimum Necessary Access

Enforce minimum-necessary access at the query level by returning only required fields:

```cypher
MATCH (p:Patient {id: $id})
RETURN p.name, p.date_of_birth
```

Combined with per-database RBAC, this ensures users only reach data relevant to their role.

---

## Audit Controls (§164.312(b))

### Audit Events

NornicDB logs all security-relevant events:

| Event | Logged Data |
|---|---|
| Authentication | User, IP, timestamp, success/failure |
| Data Access | User, database, query, timestamp |
| Data Modification | User, database, operation, timestamp |
| Data Export | User, format, record count |
| Configuration Changes | User, setting, old/new value |
| Security Events | Event type, severity, details |

### Audit Log Format

```json
{
  "event_id": "evt_abc123",
  "timestamp": "2026-04-01T10:30:00Z",
  "event_type": "DATA_ACCESS",
  "user_id": "clinician-123",
  "user_name": "Dr. Smith",
  "database": "patient_records",
  "action": "READ",
  "ip_address": "192.168.1.100",
  "details": "MATCH (p:Patient) RETURN p LIMIT 10"
}
```

### Retention

HIPAA requires audit records to be retained for at least 6 years:

```yaml
audit:
  enabled: true
  retention_days: 2555  # 7 years (exceeds HIPAA 6-year minimum)
  alert_on_failures: true
  integrity:
    enabled: true
    algorithm: SHA-256
    chain: true  # Hash chain for tamper detection
```

---

## Transmission Security (§164.312(e))

### TLS Configuration

All connections should use TLS 1.2 or higher:

```yaml
tls:
  enabled: true
  min_version: TLS1.2
  preferred_version: TLS1.3
  cipher_suites:
    - TLS_AES_256_GCM_SHA384
    - TLS_CHACHA20_POLY1305_SHA256
```

### Certificate Setup

```bash
# Generate certificates (use your CA in production)
openssl req -x509 -nodes -days 365 -newkey rsa:4096 \
  -keyout server.key -out server.crt
```

In production, use certificates issued by your organization's certificate authority or a trusted public CA.

---

## Integrity Controls (§164.312(c))

NornicDB protects data integrity through:

- **Encryption authentication** — AES-256-GCM provides authenticated encryption, detecting any tampering with stored data
- **Hash-chained audit logs** — Each audit entry includes a hash of the previous entry, making it detectable if logs are modified or deleted
- **WAL integrity** — The write-ahead log uses checksums to detect corruption during recovery

---

## Breach Notification (§164.408)

### Detection

Configure security alerting to detect potential breaches:

```yaml
audit:
  alert_on_failures: true
  alert_on_bulk_access: true
  bulk_access_threshold: 100  # Alert on large data reads
```

Review audit logs regularly and integrate alerts with your organization's incident response workflow.

### Response

Use the audit log JSONL file (default `/var/log/nornicdb/audit.log`) to investigate incidents:

```bash
# Filter all access during an incident window
jq -c 'select(.timestamp >= "2026-03-01" and .timestamp < "2026-03-15")' \
  /var/log/nornicdb/audit.log
```

---

## Business Associate Agreements

When deploying NornicDB:

1. **Self-Hosted** — You are the covered entity; NornicDB is software you operate
2. **Cloud-Hosted** — Ensure you have a BAA with your cloud provider (AWS, GCP, Azure)
3. **Managed Service** — Require a BAA from the service provider

---

## Compliance Checklist

### Technical Safeguards

- [ ] Enable TLS 1.2+ for all connections
- [ ] Enable encryption at rest (AES-256-GCM)
- [ ] Configure RBAC with minimum-necessary permissions
- [ ] Enable comprehensive audit logging
- [ ] Set audit log retention to 6+ years
- [ ] Enable hash-chained audit log integrity
- [ ] Configure security alerting
- [ ] Set session timeouts and lockout policies

### Administrative Safeguards

- [ ] Document security policies and procedures
- [ ] Train workforce on data handling
- [ ] Establish incident response procedures
- [ ] Conduct regular risk assessments
- [ ] Maintain business associate agreements
- [ ] Review audit logs on a regular schedule

---

## Complete Configuration Example

```yaml
# HIPAA-aligned NornicDB configuration
database:
  encryption_enabled: true

tls:
  enabled: true
  min_version: TLS1.2

auth:
  enabled: true
  session_timeout: 15m
  max_failed_attempts: 3
  lockout_duration: 30m

audit:
  enabled: true
  retention_days: 2555
  alert_on_failures: true
  integrity:
    enabled: true
    algorithm: SHA-256
    chain: true

rbac:
  enabled: true
  default_role: none
```

---

## See Also

- **[Encryption](encryption.md)** — Encryption at rest configuration
- **[RBAC](rbac.md)** — Role-based access control
- **[Audit Logging](audit-logging.md)** — Audit log configuration and queries
- **[GDPR Compliance](gdpr-compliance.md)** — EU data protection requirements
- **[SOC2 Compliance](soc2-compliance.md)** — Service organization controls
