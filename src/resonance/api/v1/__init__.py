"""API v1 router aggregation."""

import fastapi

import resonance.api.v1.account as account_module
import resonance.api.v1.admin as admin_module
import resonance.api.v1.artists as artists_module
import resonance.api.v1.auth as auth_module
import resonance.api.v1.calendar_feeds as calendar_feeds_module
import resonance.api.v1.events as events_module
import resonance.api.v1.generators as generators_module
import resonance.api.v1.history as history_module
import resonance.api.v1.matching as matching_module
import resonance.api.v1.playlists as playlists_module
import resonance.api.v1.sync as sync_module
import resonance.api.v1.tracks as tracks_module

router = fastapi.APIRouter(prefix="/api/v1")
router.include_router(account_module.router)
router.include_router(admin_module.router)
router.include_router(artists_module.router)
router.include_router(auth_module.router)
router.include_router(calendar_feeds_module.router)
router.include_router(events_module.router)
router.include_router(generators_module.router)
router.include_router(history_module.router)
router.include_router(matching_module.router)
router.include_router(playlists_module.router)
router.include_router(sync_module.router)
router.include_router(tracks_module.router)
