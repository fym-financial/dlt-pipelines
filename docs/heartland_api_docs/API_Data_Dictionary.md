# Heartland API data dictionary

Converted from `API_Data_Dictionary.xlsx` and verified against a live response captured on July 13, 2026. Blank descriptions and details that are not present in either source are explicitly marked as unspecified.

## Authentication

- Purpose: log in to obtain a token
- Endpoint: `https://api.hnlicagent.com/api/auth/login`
- HTTP method: `POST`
- Content type: `application/json`
- Username noted in the workbook: `FYMUser`
- Response: a JWT returned as plain text, not as a JSON object

Login request template:

```json
{
  "Username": "user",
  "Password": "pass"
}
```

## Inforced policies

- Endpoint: `https://api.hnlicagent.com/api/FYM/GetPolicies`
- HTTP method: `GET`
- Authentication: `Authorization: Bearer <token>`
- Response content type: JSON
- Response shape: a top-level array of policy objects
- Observed sample size: 222 objects, each containing the same 36 properties

Example request:

```bash
curl -sS \
  'https://api.hnlicagent.com/api/FYM/GetPolicies' \
  -H "Authorization: Bearer $TOKEN"
```

### Response fields

All observed JSON values are strings. Property names below use the exact camelCase spelling returned by the API.

| JSON property | Observed format | Description |
| --- | --- | --- |
| `polNo` | String identifier | Policy number. |
| `agtCode` | Numeric string | Agent writing number. |
| `agtFirstName` | String | Agent first name. |
| `agtLastName` | String | Agent last name. |
| `amrStatus` | String enum | AMR status; includes a 10-day grace period before a policy lapses. Observed: `Active`, `Cancelled`, `Not Taken`. |
| `appDate` | `M/D/YYYY` string | Date the application was signed. |
| `effDate` | `M/D/YYYY` string | Effective date of the policy. |
| `birthDate` | `M/D/YYYY` string | Client birth date. |
| `premium` | Decimal string | Policy premium. |
| `plan` | Numeric string | Plan code. |
| `productDesc` | String | Full plan description. |
| `type` | String code | Type of product. Observed: `HHC`, `HIP`. |
| `issueState` | Two-letter code string | State in which the policy was issued. |
| `entryDate` | `M/D/YYYY` string | Date entered into AMR. |
| `firstName` | String | Client first name. |
| `lastName` | String | Client last name. |
| `share` | Decimal string | Split percentage. Usually `1`, but a split may produce multiple rows with a decimal value. |
| `initialPaidDate` | `M/D/YYYY` string | Initial paid date. `1/1/1900` appears to be a sentinel for no actual paid date. |
| `clientAddress` | String | Client street address. |
| `clientAddress2` | String | Second client address line; may be empty. |
| `clientCity` | String | Client city. |
| `clientState` | ZIP-like string | Despite its name, this property contained the client ZIP/postal code in every observed object. |
| `clientZip` | Two-letter code string | Despite its name, this property contained the client state code in every observed object. |
| `clientEmail` | String | Client email address; may be empty or contain a placeholder. |
| `clientPhone` | String | Client phone number; formatting is not guaranteed to be valid or consistent. |
| `paidToDate` | `M/D/YYYY` string | Date through which the policy is paid. |
| `draftDay` | Numeric string | Selected day of the month for drafting. `99` appears to be a sentinel rather than a calendar day. |
| `hnlStatus` | String enum | Current status without a grace period. Observed: `Active`, `Cancelled`, `Not Taken`, `Pending Lapse`. |
| `returnDescripton` | String | Description of a returned payment; may be empty. The misspelling is present in the API response. |
| `chargeBackDt` | `M/D/YYYY` string | Date the chargeback occurred; may be empty. |
| `upline` | String | Agent's immediate upline, observed as a name and agent code in one string. |
| `issAge` | Integer string | Client age at issue. |
| `endDate` | `M/D/YYYY` string | Agent contract end date. `1/1/2060` is used for an active/non-ended contract. |
| `appGuid` | UUID string | Unique application identifier in HNL's systems. |
| `appType` | String enum | Indicates whether the application was electronic or paper. Only `eApp` was observed. |
| `writingSplit` | String enum | Indicates whether the agent is the writing agent or split agent. Only `Writing` was observed. |

### Data-quality and ingestion notes

- Treat every source value as a nullable string at extraction time and perform typed conversion during normalization.
- Swap or rename `clientState` and `clientZip` during normalization; their returned contents are consistently reversed.
- Parse dates with the `M/D/YYYY` convention. Empty strings and known sentinel dates must be handled before conversion.
- Interpret `initialPaidDate = "1/1/1900"`, `endDate = "1/1/2060"`, and `draftDay = "99"` as business sentinels, not ordinary values, subject to confirmation from Heartland.
- Preserve source strings before cleaning because whitespace, casing, phone formatting, ZIP formatting, and email placeholders are inconsistent.
- The response includes sensitive personal information such as names, birth dates, postal addresses, email addresses, and phone numbers. Do not commit raw responses or use real records in tests.

### Warehouse tables

The load process maintains insert-only row-version history:

- `raw.heartland_inforced_policy_snapshot`: disposable DLT staging table containing the latest full API response as text.
- `raw.heartland_inforced_policy`: canonical insert-only history. A deterministic hash across all 36 business fields prevents exact row versions from being appended more than once.
- `typed.heartland_inforced_policy`: insert-only typed history. Dates are PostgreSQL `date`; `premium` and `share` are `numeric`; `iss_age` is `smallint`; all other business fields remain text.
- `typed.heartland_inforced_policy_latest`: newest observed version for each `pol_no`, `agt_code`, and `writing_split` combination.
- The typed table maps raw `client_zip` to `client_state` and raw `client_state` to `client_zip` to correct the API's reversed payload values.
- Empty strings become `NULL` in typed data. Invalid dates and invalid numeric values also become `NULL` rather than failing the full load.
- Canonical raw and typed rows are never updated or deleted by this process. An unchanged response appends zero rows; a changed policy is appended as a new version.

Run the complete load:

```bash
infisical run --env=dev -- uv run dlt-pipeline load-heartland
```

## Source notes

- The source workbook uses the labels `InforcedPolicy` and `Inforced Policies`; they are retained here even though “in-force policies” is the conventional spelling.
- No request parameters, pagination behavior, error schema, formal nullability rules, or complete allowed-value lists are defined in the workbook.
