"""API v1 router aggregation."""

import fastapi

import resonance.api.v1.auth as auth_module

router = fastapi.APIRouter(prefix="/api/v1")
router.include_router(auth_module.router)
