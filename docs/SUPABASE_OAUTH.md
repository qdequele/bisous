# Connect with Supabase (OAuth): design

Status: **accepted and built** (2026-11-20). Section 11 lists where the build differs from this design.

Today a Supabase sync needs the project's direct connection string, with the
`postgres` password, pasted into the Lab console. This page designs a
"Connect with Supabase" button instead. The user approves our OAuth app and
picks a project. The Lab then creates a dedicated replication role on that
project through Supabase's Management API, with a password nobody sees, and
hands the engine a connection spec.

Repos: the Lab (`meilisearch-lab`) does almost all of it. The engine gets one
new source option (an externally managed publication) and one new check
(row-level security). The cross-repo contract is in `LAB_INTEGRATION.md`,
section "Supabase OAuth".

## 1. What Supabase gives us (checked 2026-11-20)

Sources: the live spec at `https://api.supabase.com/api/v1-json`, the guide
"Build a Supabase OAuth integration" with its scopes page, the API
introduction, "Connecting to Postgres", and the manual replication setup page
and FAQ.

**OAuth app.** It is registered in a Supabase organization (Organization
settings → OAuth Apps). It uses authorization code with PKCE (S256), which
Supabase recommends but does not require.

- Authorize: `GET https://api.supabase.com/v1/oauth/authorize` with
  `client_id`, `redirect_uri`, `response_type=code`, `state`,
  `code_challenge` and `code_challenge_method=S256`. `organization_slug` is
  optional and pre-selects an organization. `redirect_uri` and `state`
  together must stay under 4 kB.
- Token: `POST /v1/oauth/token` (form-encoded), with the client sent as HTTP
  Basic. `grant_type` is `authorization_code` or `refresh_token`. The answer
  is `access_token`, `refresh_token`, `expires_in`, `token_type` and
  `project_refs`; `project_refs: null` means all projects. The token lifetime
  is not documented, so read `expires_in`. Refresh-token rotation is not
  documented either: always store the `refresh_token` that comes back.
- Revoke: `POST /v1/oauth/revoke`, a JSON body `{client_id, client_secret,
  refresh_token}`, answers 204. It is marked beta.
- Scopes are set on the app, not per request. Changing them makes existing
  users re-authorize. A grant covers one organization: the user picks it on
  the consent screen.
- When the user revokes the app on Supabase's side, refresh fails and access
  tokens stop working (401). There is no webhook.
- Rate limit: 120 requests per minute, per endpoint and per project or
  organization. **For an OAuth app the limit is shared by every user of the
  app.** A 429 answer carries `X-RateLimit-Reset`.

**Endpoints we use** (each with its scope):

| Call | Scope | Use |
|---|---|---|
| `GET /v1/organizations` | `organizations:read` | Name the connected organization |
| `GET /v1/projects` | `projects:read` | Project picker: `ref`, `name`, `region`, `status`, `organization_slug`, `database.{host, version}` |
| `POST /v1/projects/{ref}/database/query` | `database:write` | Run the provisioning SQL. Body `{query, parameters?, read_only?}`, answers 201 with an array of rows. Marked beta. |
| `POST /v1/projects/{ref}/database/query/read-only` | `database:read` | Pre-flight reads (runs as `supabase_read_only_user`, with schema-qualified names) |
| `GET /v1/projects/{ref}/billing/addons` | none listed | Shown only: whether the IPv4 add-on is on |

**Scopes the app requests:** `organizations:read`, `projects:read`,
`database:read`, `database:write`. Nothing else.

**Not offered by the API:** creating a role, or reading the `postgres`
password. `PATCH /database/password` *resets* `postgres`, which would break
the user's own clients: **never call it**. Roles are therefore created with
SQL through `/database/query`. The role that SQL runs as is not documented
(reported to be `postgres`); the pre-flight checks `current_user`.

**Replication facts:**

- Only the direct connection (`db.<ref>.supabase.co:5432`) supports logical
  replication. Supabase says plainly that the shared and the dedicated
  poolers will not work for it.
- The direct host is IPv6-only unless the project has the IPv4 add-on. The
  shared pooler is IPv4-only. Our box reaches IPv6 since 2026-11-20, so the
  direct host works on every plan.
- Supabase's setup page says `postgres` can create a role `WITH REPLICATION`
  on **Postgres 17+**, and that older versions must replicate as `postgres`
  itself. Another of their pages says it works without naming a version.
  We go by what the project actually does: see §4.

## 2. User experience

New sync wizard (`/connect/<slug>/syncs/new`), Supabase source, step 1:

1. **Connect with Supabase** is the main button. "Use a connection string
   instead" opens today's form, which stays as the fallback.
2. With no Supabase connection for the team, the button goes to Supabase's
   consent screen. The user picks an organization and approves. They come
   back to `/connect/supabase/callback`, which completes the grant and
   returns them to the wizard (`?source=supabase`).
3. With a connection, the wizard shows the **project picker**: the projects
   of the connected organization, each with its region and status. A project
   that is not `ACTIVE_HEALTHY` (paused, restoring…) is shown but disabled,
   with the reason. "Switch organization" re-runs the consent.
4. When the user picks a project, the Lab **provisions** it (§4): about a
   second, with a spinner. It fails with an actionable message (Postgres
   older than 17, `wal_level` not logical, project paused).
5. The wizard goes on to table selection, as today. The tables come from the
   engine's `/sources/discover`, run with the provisioned role. Check,
   mapping, destination and review are unchanged.

Team settings (Integrations) list the Supabase connection: the organization,
who connected it and when, the provisioned projects, and **Disconnect**.

## 3. Where things live

| Data | Where | Notes |
|---|---|---|
| OAuth access and refresh tokens | Lab, `supabase_connections` (one row per team), `encrypts` | Never sent to the console or the engine |
| Provisioned role name and password | Lab, `supabase_projects` (one row per team and project ref), password `encrypts` | The browser never sees it |
| Connection spec with the password | Engine sync spec, encrypted with `CONNECT_SECRET_KEY` | As today for pasted specs |
| Publication name and tables per sync | Lab, `supabase_publications` | The Lab owns the publication (§5) |

Tokens stay in the Lab: the engine has no Management API code and no
per-tenant OAuth state. The engine only ever sees a plain Postgres spec.

**The console never handles the password.** It sends a Lab-only source
form, `{"kind": "supabase", "config": {"project": "<supabase_projects
id>"}}`, on `/sources/check`, `/sources/discover`, `POST /syncs` and
`PUT /syncs/{id}`. The Lab proxy replaces it with the real spec, exactly as
`ConnectDestinations` does for `destination: {"instance": …}`. On answers,
the Lab maps a spec whose host and user match a provisioned project back to
`{"project": id}`.

## 4. Provisioning a project

`SupabaseProvisioner.call(connection, ref)` is idempotent: it reuses the
team's `supabase_projects` row for that ref when the role still exists.

**Pre-flight** (`/database/query`):

```sql
select current_user,
       current_setting('server_version_num')::int as version,
       current_setting('wal_level') as wal_level,
       r.rolreplication, r.rolbypassrls, r.rolcreaterole
from pg_roles r where r.rolname = current_user;
```

It fails with a clear code when:

- `wal_level` is not `logical` (`supabase_wal_level`);
- `current_user` cannot create roles (`supabase_cannot_create_role`);
- `current_user` lacks `REPLICATION` and the version is below 17
  (`supabase_replication_unsupported`). The message offers the
  connection-string form, which works with the `postgres` user.

**Role.** The name is `meili_connect_<8 random chars>`; it never reuses a
role it did not create. One SQL call does:

```sql
create role meili_connect_x7k2p9qa with login replication [bypassrls]
  password 'SCRAM-SHA-256$4096:…';   -- verifier computed in the Lab
comment on role meili_connect_x7k2p9qa is 'meili-connect:lab:<team id>';
```

- The password is 32 random bytes, base64url. The Lab sends Supabase only a
  **SCRAM-SHA-256 verifier** it computes itself (PBKDF2 in Ruby), so the
  clear password never leaves the Lab towards Supabase, and never reaches
  Supabase's query logs.
- `BYPASSRLS` is added when `current_user` has it (Supabase's `postgres`
  does). Without it, the snapshot of a table with row-level security returns
  only the rows the policies allow, **silently**. Supabase enables RLS on
  tables created in the dashboard, so this matters. When the role cannot get
  `BYPASSRLS`, the engine's new RLS check (§7) fails those tables with an
  explanation.
- The role gets no `CREATE` on the database. It owns nothing, so dropping it
  is clean.

**Grants** are set per sync write, not at provisioning: `grant usage on
schema <s>` and `grant select on <s>.<t>` for the mapped tables. Discover
does not need them, because the engine lists tables from the catalogs.

**Connection spec** given to the engine:

```json
{"host": "db.<ref>.supabase.co", "port": 5432, "database": "postgres",
 "user": "meili_connect_x7k2p9qa", "password": "<generated>",
 "sslmode": "require",
 "publication_name": "meili_connect_lab_<12 chars>", "manage_publication": false}
```

The host comes from `GET /v1/projects` (`database.host`); Supabase does not
document the name pattern as stable.

## 5. Publications: the Lab owns them

The engine creates its publication itself today: `CREATE PUBLICATION … FOR
TABLE …`, then `ALTER PUBLICATION … SET TABLE` when streams change. Postgres
lets only **the table owner** (or a superuser) add a table to a publication.
On Supabase the owner is `postgres`, so a least-privilege role cannot do it.
Making the role a member of `postgres` would undo the whole point.

So on the Lab path, the **Lab** creates and changes the publication as
`postgres` through the Management API, and the engine only uses it:

- **Engine (new option).** `manage_publication: false` (default `true`) on
  `postgres`/`supabase` sources, with `publication_name` required:
  - never creates, alters or drops the publication, and needs no marker on
    it;
  - `check` fails when the publication is missing, or names each mapped
    table it does not cover;
  - `open` fails with `Config` when a table is missing from the publication;
  - `teardown` drops only the slot.

  The slot keeps its default name, derived from the sync id, so the
  ownership rule for slots does not change. The role can drop its own slot,
  because `REPLICATION` is enough.
- **Lab, `POST /syncs`.** Before forwarding, the Lab grants `SELECT` on the
  mapped tables and creates `meili_connect_lab_<random>` `FOR TABLE` them,
  with `publish_via_partition_root = true` and the comment
  `meili-connect:lab:<team id>`. It forwards the spec with
  `publication_name`. When the answer comes back, it records the sync id
  against the publication. If the engine refuses the sync, it drops the
  publication again.
- **Lab, `PUT /syncs/{id}`** with other tables: `ALTER PUBLICATION … SET
  TABLE …` and the new grants first, then forward. A table no other sync of
  that project maps gets its `SELECT` revoked after the engine accepts. The
  publication name stays the same, so the engine does not treat it as a new
  origin.
- **Lab, `DELETE /syncs/{id}`.** The engine drops the slot (as it does
  today). After a 204, the Lab drops the publication and revokes the grants
  no other sync needs.

## 6. Lifecycle

**Token refresh.** `SupabaseConnection#access_token!` refreshes under
`with_lock` two minutes before expiry, as `CloudConnection` does. A 401 or
`invalid_grant` marks the connection `revoked`.

**Password rotation.** A recurring job runs every 90 days per project, and
"Rotate" is offered on the project row. It:

1. generates a new password and runs `alter role … password '<verifier>'`;
2. sends each sync of that project a `GET` then a `PUT` to the engine with
   the new password. This is a restart, not a new origin, so the checkpoint
   is kept.

A connection that is already open survives the `ALTER ROLE`, because Postgres
checks the password only when a connection logs in. A reconnect between
steps 1 and 2 fails transiently and is retried. If step 2 fails, the job
retries; the new password is stored before step 1.

**Disconnect** (team admin, in Integrations):

1. Refused with `409 supabase_in_use` while a sync uses a project of the
   connection. The console lists those syncs and offers to delete them
   first; deleting a sync drops its slot and publication.
2. For each project: drop any `meili_connect_lab_*` publication carrying our
   comment, then `drop owned by <role>; drop role <role>`. `DROP OWNED` is
   what revokes the grants. Slots are not dropped here: each sync drops its
   own slot when it is deleted, and a slot has no owner to find it by.
3. Revoke the token at Supabase (`/v1/oauth/revoke`), then delete the rows.

A failed step leaves the rows in place with the error, so Disconnect can be
retried.

**User revokes the app on Supabase's side.** We learn it from the next 401:
the connection becomes `revoked`.

- **Running syncs keep running.** The role and its password live in the
  user's database. Revoking an OAuth app does not remove roles it created.
- Everything that needs the Management API is blocked until they reconnect:
  new syncs, table changes and rotation. The console says so.
- Deleting a sync still works in the engine (it drops its slot), but the
  Lab cannot drop the publication or the role. The console shows the SQL to
  run in Supabase's editor. The docs state plainly that revoking the app
  does not cut database access, and that disconnecting in the Lab (or
  dropping the role) does.

**Project paused on a free plan.** No change: the engine already reports
`source_paused` for Supabase.

## 7. Engine changes (`qdequele/bisous`)

1. `manage_publication: false`, as §5 describes: config, check, open,
   teardown, the OpenAPI `SourceSpec` note, and tests (unit tests plus a live
   test gated on `PG_TEST_URL`).
2. **RLS check.** In `check`, a mapped table with `relrowsecurity`, when the
   role is neither its owner, nor a superuser, nor `BYPASSRLS`, is
   `failed`: "row-level security is on and the role does not bypass it: the
   snapshot would only copy the rows its policies allow; grant BYPASSRLS or
   use the table owner". This fixes a silent gap that exists today for any
   non-owner role, pasted or provisioned.

This is a contract change, so the engine PR merges and releases first. The
Lab vendors the new `openapi.yaml`, and the Lab PR that uses
`manage_publication` merges after that.

## 8. Lab changes (`meilisearch-lab`)

- **Migrations** (additive):
  - `supabase_connections`: `account_id` (unique), `organization_slug`,
    `organization_name`, `access_token`, `refresh_token`, `expires_at`,
    `status`, `connected_by_id`.
  - `supabase_projects`: `account_id`, `supabase_connection_id`, `ref`,
    `name`, `host`, `role_name`, `role_password`, `password_rotated_at`,
    `status`; unique on `(account_id, ref)`.
  - `supabase_publications`: `supabase_project_id`, `name`, `sync_id`,
    `tables` (jsonb).
- **Services:**
  - `SupabaseOauth` (authorize URL, PKCE, encrypted state, token exchange,
    refresh, revoke) and `SupabaseApi` (Management API client, plain
    `Net::HTTP`, typed errors for 401, 403, 429 and 5xx), modelled on
    `MeilisearchCloud`.
  - `SupabaseProvisioner`, `SupabasePublications`, and `SupabaseScram` (the
    verifier).
  - `ConnectSupabaseSources`, the proxy rewrite next to
    `ConnectDestinations`.
- **Routes** (account scope):
  - `GET /account/supabase` returns `{enabled, connection}`;
  - `POST /account/supabase/authorize` and `POST /account/supabase/callback`;
  - `GET /account/supabase/projects`;
  - `POST /account/supabase/projects/:ref` (provision);
  - `POST /account/supabase/projects/:ref/rotate`;
  - `DELETE /account/supabase`.
- **Roles:** connect and disconnect need owner or admin, as Meilisearch
  Cloud does. Picking or provisioning a project needs member, the role
  needed to create a sync.
- **Jobs:** `RotateSupabasePasswordsJob` (daily; it rotates what is due).
- **Console:**
  - a "Connect with Supabase" button and the project picker in
    `connection-form`, for the `supabase` kind;
  - the callback page `/connect/supabase/callback`;
  - the `supabase` source config `{project}`;
  - hooks in `lib/connect/`;
  - the Integrations card.
- **Tests** (Minitest + WebMock on `api.supabase.com`; vitest):
  - the OAuth state (tamper, expiry, another user);
  - refresh and revocation;
  - the provisioner SQL and its pre-flight refusals;
  - the SCRAM verifier against a known vector;
  - the proxy rewrite on all four calls, including the publication on
    create, change and delete;
  - disconnect while in use;
  - the wizard button and picker.
- **Docs:** `connect/supabase.mdx` (the new path first, with the
  connection-string fallback) and `operate/integrations.mdx` (registering
  the app). The env vars go in `operate/environment.mdx`, `.env.example` and
  the CLAUDE.md table.
- **Env:** `SUPABASE_OAUTH_CLIENT_ID` and `SUPABASE_OAUTH_CLIENT_SECRET`,
  plus `SUPABASE_API_URL` (default `https://api.supabase.com`, overridden in
  tests). With no client id, the feature is off and the button is hidden.

## 9. What the operator must do (not code)

1. In the Meilisearch Supabase organization, go to Organization settings →
   OAuth Apps → Add application:
   - name "Meilisearch Lab", with the website and logo;
   - redirect URI `https://<console host>/connect/supabase/callback`, plus
     `http://localhost:3000/connect/supabase/callback` on a separate dev
     app;
   - scopes `organizations:read`, `projects:read`, `database:read` and
     `database:write`.
2. Put `SUPABASE_OAUTH_CLIENT_ID` and `SUPABASE_OAUTH_CLIENT_SECRET` in
   `/etc/scrapix/scrapix.env` on the box before the Lab PR merges, and in
   `.env` for local development.
3. Release the engine with `manage_publication` before merging the Lab PR.

## 10. Risks and open questions

- **Beta endpoints.** `/database/query` and `/oauth/revoke` are marked beta.
  The client isolates them, and the connection-string path stays.
- **Postgres below 17.** A dedicated role may be impossible there (§4); those
  projects fall back to the connection string. How many Supabase projects
  still run 15 is unknown. The pre-flight tells us, and we can count it.
- **Shared rate limit** (120 per minute per endpoint, for the whole app).
  Provisioning is about 3 calls. The project list is cached per team for 60
  seconds.
- **We hold org-wide `database:write` tokens.** That is a high-value secret.
  It is encrypted at rest, used only by the services above, never returned
  by the API. The Lab has no team audit log yet: the SQL it runs is in its
  logs only through errors (see section 11). Supabase may later let users limit a grant to
  some projects (`project_refs`); we honour it when it is not null.
- **WAL retention.** We do not change `max_slot_wal_keep_size`: that
  restarts the user's database. The engine's lag guard already holds a sync
  before it hurts.

## 11. As built

Where the build differs from the sections above:

- **No IPv4 add-on lookup.** The hosted engine reaches IPv6, so the
  `billing/addons` call was left out: it would cost one rate-limited call per
  project for a hint nobody needs.
- **The check grants SELECT.** `POST /sources/check` with a project source
  grants `SELECT` on the probed tables first, so the check reports on the
  role as the sync will use it, RLS included. The Lab answers the check's
  `publication` item itself ("the Lab creates the publication when the sync
  is saved"), because the publication is only made with the sync. `SELECT`
  on a table no sync maps is revoked after the next sync write or delete of
  that project.
- **The publication of a failed call.** If the destination is refused before
  the engine call, or the engine refuses the sync, the new publication is
  dropped again, and a changed one gets its tables back. After an engine
  timeout a new publication is kept as an orphan, because the engine may hold
  the sync. Orphans are listed with their `DROP PUBLICATION` SQL on the team
  page.
- **`slot_name` is refused** with `manage_publication: false`. Only the slot
  name derived from the sync id marks a slot as the sync's, so a custom name
  would leak the slot.
- **One organization per team.** "Switch organization" authorizes again and
  replaces the tokens. Provisioned projects stay, and they work again as soon
  as the new grant reaches them.
- **Console.** The Supabase source has a third mode, "Your Supabase account",
  which is the default. Its config is `{"project": "<id>"}` and nothing else.
  The team page has a Supabase card: the projects, password rotation (owner
  or admin), cleanup SQL and Disconnect.

