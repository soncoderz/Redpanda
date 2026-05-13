# Add Endpoint

Scaffold a new API endpoint for this Hono + Restate project. The user will provide the resource name (e.g., "patient", "service").

## Steps

1. **Ask** the user for:
   - Resource name (singular, e.g., "patient")
   - CRUD operations needed (create, read, update, delete, list)
   - Whether it needs a Restate Virtual Object handler or is MongoDB-only

2. **Create files** following existing patterns:

### Model (`models/<resource>.model.ts`)
- Define Zod schemas: `<Resource>Input`, `<Resource>State` (if Restate), or `<Resource>Schema` (if Mongoose-only)
- Export inferred types with `z.infer`
- Follow the pattern in `models/appointment.model.ts`

### Mongoose Schema (`models/<resource>.schema.ts`)
- Create Mongoose schema matching Zod model fields
- Export the Mongoose model
- Follow the pattern in `models/appointment.schema.ts`

### Repository (`models/<resource>.repository.ts`)
- Pure functions (no class): `upsert<Resource>()`, `list<Resource>s()`, `find<Resource>ById()`
- Follow the pattern in `models/appointment.repository.ts`

### Validation (`validation/<resource>.validation.ts`)
- Extend base model schemas for API-specific validation (add optional fields like `id`, `idempotencyKey`)
- Follow the pattern in `validation/appointment.validation.ts`

### Controller (`controllers/<resource>.controller.ts`)
- Hono handlers: `create<Resource>`, `get<Resource>`, `update<Resource>`, `delete<Resource>`, `list<Resource>s`
- Parse input with Zod `.parse()`
- Call Restate client (if applicable) or repository directly
- Return JSON responses with appropriate status codes
- Follow the pattern in `controllers/appointment.controller.ts`

### Routes (`routes/<resource>.routes.ts`)
- Create `create<Resource>Routes()` function returning a `Hono` router
- RESTful: `POST /`, `GET /`, `GET /:id`, `PATCH /:id`, `DELETE /:id`
- Follow the pattern in `routes/appointment.routes.ts`

3. **Register routes** in `app.ts`:
   - Import `create<Resource>Routes`
   - Add `app.route("/api/<resources>", create<Resource>Routes())`

4. **If Restate Virtual Object needed**:
   - Create handler in `services/restate/<resource>.handler.ts`
   - Register in `config/restate.ts`
   - Follow patterns from `services/restate/appointment.handler.ts`

5. **Run** `npx tsc --noEmit` to verify no type errors.

## Conventions
- File names: `kebab-case.ts`
- All imports use `.js` extension (ESM)
- Comments in Vietnamese
- Zod for validation, Pino for logging
