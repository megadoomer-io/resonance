"""API v1 router aggregation."""

import fastapi

import resonance.api.v1.account as account_module
import resonance.api.v1.admin as admin_module
import resonance.api.v1.auth as auth_module
import resonance.api.v1.sync as sync_module

router = fastapi.APIRouter(prefix="/api/v1")
router.include_router(account_module.router)
router.include_router(admin_module.router)
router.include_router(auth_module.router)
router.include_router(sync_module.router)
