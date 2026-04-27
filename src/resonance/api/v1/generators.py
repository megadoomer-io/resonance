"""Generator profile API routes — CRUD and playlist generation trigger."""

from __future__ import annotations

import uuid
from typing import Annotated, Any

import fastapi
import pydantic
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import structlog

import resonance.dependencies as deps_module
import resonance.generators.parameters as params_module
import resonance.models.generator as generator_models
import resonance.models.task as task_models
import resonance.types as types_module

logger = structlog.get_logger()

router = fastapi.APIRouter(
    prefix="/generator-profiles", tags=["generators"], redirect_slashes=False
)


class CreateProfileRequest(pydantic.BaseModel):
    """Request body for creating a generator profile."""

    name: str
    generator_type: types_module.GeneratorType
    input_references: dict[str, str]
    parameter_values: dict[str, int] = pydantic.Field(default_factory=dict)


class UpdateProfileRequest(pydantic.BaseModel):
    """Request body for updating a generator profile."""

    name: str | None = None
    parameter_values: dict[str, int] | None = None
    input_references: dict[str, str] | None = None


class GenerateRequest(pydantic.BaseModel):
    """Request body for triggering playlist generation."""

    freshness_target: int | None = None
    max_tracks: int = 30


def validate_profile_inputs(request: CreateProfileRequest) -> None:
    """Validate that required inputs are present for the generator type.

    Args:
        request: The create profile request to validate.

    Raises:
        ValueError: If a required input is missing.
    """
    config = params_module.GENERATOR_TYPE_CONFIG.get(request.generator_type)
    if config is None:
        return
    for required in config.required_inputs:
        if required not in request.input_references:
            msg = f"Missing required input: {required}"
            raise ValueError(msg)


@router.post(
    "/",
    summary="Create generator profile",
    description="Create a new generator profile (saved playlist recipe).",
)
async def create_profile(
    body: CreateProfileRequest,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    """Create a new generator profile.

    Args:
        body: The profile creation request body.
        user_id: The authenticated user's ID.
        db: The async database session.

    Returns:
        A dict with the created profile's ID and status.

    Raises:
        HTTPException: 400 if required inputs are missing.
    """
    try:
        validate_profile_inputs(body)
    except ValueError as exc:
        raise fastapi.HTTPException(status_code=400, detail=str(exc)) from exc

    profile = generator_models.GeneratorProfile(
        user_id=user_id,
        name=body.name,
        generator_type=body.generator_type,
        input_references=body.input_references,
        parameter_values=body.parameter_values,
    )
    db.add(profile)
    await db.commit()

    logger.info(
        "generator_profile_created",
        profile_id=str(profile.id),
        user_id=str(user_id),
        generator_type=body.generator_type.value,
    )

    return {
        "status": "created",
        "profile_id": str(profile.id),
        "name": profile.name,
        "generator_type": profile.generator_type.value,
    }


@router.get(
    "/",
    summary="List generator profiles",
    description="List all generator profiles for the authenticated user.",
)
async def list_profiles(
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> list[dict[str, Any]]:
    """List all generator profiles for the current user.

    Args:
        user_id: The authenticated user's ID.
        db: The async database session.

    Returns:
        A list of profile summary dicts.
    """
    stmt = (
        sa.select(generator_models.GeneratorProfile)
        .where(generator_models.GeneratorProfile.user_id == user_id)
        .order_by(generator_models.GeneratorProfile.created_at.desc())
    )
    result = await db.execute(stmt)
    profiles = result.scalars().all()

    return [
        {
            "id": str(profile.id),
            "name": profile.name,
            "generator_type": profile.generator_type.value,
            "input_references": profile.input_references,
            "parameter_values": profile.parameter_values,
            "created_at": profile.created_at.isoformat(),
            "updated_at": profile.updated_at.isoformat(),
        }
        for profile in profiles
    ]


@router.get(
    "/{profile_id}",
    summary="Get generator profile",
    description="Get a generator profile with its generation history.",
)
async def get_profile(
    profile_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    """Get a generator profile with its generation history.

    Args:
        profile_id: The profile UUID.
        user_id: The authenticated user's ID.
        db: The async database session.

    Returns:
        A dict with profile details and generation history.

    Raises:
        HTTPException: 404 if profile not found.
    """
    stmt = sa.select(generator_models.GeneratorProfile).where(
        generator_models.GeneratorProfile.id == profile_id,
        generator_models.GeneratorProfile.user_id == user_id,
    )
    result = await db.execute(stmt)
    profile = result.scalar_one_or_none()

    if profile is None:
        raise fastapi.HTTPException(status_code=404, detail="Profile not found")

    # Load generation history
    gen_stmt = (
        sa.select(generator_models.GenerationRecord)
        .where(generator_models.GenerationRecord.profile_id == profile_id)
        .order_by(generator_models.GenerationRecord.created_at.desc())
        .limit(20)
    )
    gen_result = await db.execute(gen_stmt)
    generations = gen_result.scalars().all()

    return {
        "id": str(profile.id),
        "name": profile.name,
        "generator_type": profile.generator_type.value,
        "input_references": profile.input_references,
        "parameter_values": profile.parameter_values,
        "auto_sync_targets": profile.auto_sync_targets,
        "created_at": profile.created_at.isoformat(),
        "updated_at": profile.updated_at.isoformat(),
        "generations": [
            {
                "id": str(gen.id),
                "playlist_id": str(gen.playlist_id),
                "parameter_snapshot": gen.parameter_snapshot,
                "freshness_target": gen.freshness_target,
                "freshness_actual": gen.freshness_actual,
                "generation_duration_ms": gen.generation_duration_ms,
                "track_sources_summary": gen.track_sources_summary,
                "created_at": gen.created_at.isoformat(),
            }
            for gen in generations
        ],
    }


@router.patch(
    "/{profile_id}",
    summary="Update generator profile",
    description="Update a generator profile's name, parameters, or input references.",
)
async def update_profile(
    profile_id: uuid.UUID,
    body: UpdateProfileRequest,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    """Update a generator profile.

    Args:
        profile_id: The profile UUID.
        body: The update request body with optional fields.
        user_id: The authenticated user's ID.
        db: The async database session.

    Returns:
        A dict with the updated profile details.

    Raises:
        HTTPException: 404 if profile not found.
    """
    stmt = sa.select(generator_models.GeneratorProfile).where(
        generator_models.GeneratorProfile.id == profile_id,
        generator_models.GeneratorProfile.user_id == user_id,
    )
    result = await db.execute(stmt)
    profile = result.scalar_one_or_none()

    if profile is None:
        raise fastapi.HTTPException(status_code=404, detail="Profile not found")

    if body.name is not None:
        profile.name = body.name
    if body.parameter_values is not None:
        profile.parameter_values = dict(body.parameter_values)
    if body.input_references is not None:
        profile.input_references = dict(body.input_references)

    await db.commit()

    logger.info(
        "generator_profile_updated",
        profile_id=str(profile_id),
        user_id=str(user_id),
    )

    return {
        "status": "updated",
        "id": str(profile.id),
        "name": profile.name,
        "generator_type": profile.generator_type.value,
        "input_references": profile.input_references,
        "parameter_values": profile.parameter_values,
    }


@router.delete(
    "/{profile_id}",
    summary="Delete generator profile",
    description="Delete a generator profile and its generation history.",
)
async def delete_profile(
    profile_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    """Delete a generator profile.

    Args:
        profile_id: The profile UUID.
        user_id: The authenticated user's ID.
        db: The async database session.

    Returns:
        A dict with deletion status.

    Raises:
        HTTPException: 404 if profile not found.
    """
    stmt = sa.select(generator_models.GeneratorProfile).where(
        generator_models.GeneratorProfile.id == profile_id,
        generator_models.GeneratorProfile.user_id == user_id,
    )
    result = await db.execute(stmt)
    profile = result.scalar_one_or_none()

    if profile is None:
        raise fastapi.HTTPException(status_code=404, detail="Profile not found")

    await db.delete(profile)
    await db.commit()

    logger.info(
        "generator_profile_deleted",
        profile_id=str(profile_id),
        user_id=str(user_id),
    )

    return {"status": "deleted", "profile_id": str(profile_id)}


@router.post(
    "/{profile_id}/generate",
    summary="Trigger playlist generation",
    description=(
        "Trigger playlist generation for a profile."
        " Creates a task and enqueues an arq job."
    ),
)
async def trigger_generation(
    profile_id: uuid.UUID,
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    body: GenerateRequest | None = None,
) -> dict[str, str]:
    """Trigger playlist generation for a profile.

    Creates a Task with PLAYLIST_GENERATION type and enqueues
    the generate_playlist arq job.

    Args:
        profile_id: The profile UUID to generate from.
        request: The FastAPI request object.
        user_id: The authenticated user's ID.
        db: The async database session.
        body: Optional generation parameters.

    Returns:
        A dict with status and task_id.

    Raises:
        HTTPException: 404 if profile not found.
    """
    stmt = sa.select(generator_models.GeneratorProfile).where(
        generator_models.GeneratorProfile.id == profile_id,
        generator_models.GeneratorProfile.user_id == user_id,
    )
    result = await db.execute(stmt)
    profile = result.scalar_one_or_none()

    if profile is None:
        raise fastapi.HTTPException(status_code=404, detail="Profile not found")

    gen_params = body or GenerateRequest()

    task = task_models.Task(
        user_id=user_id,
        task_type=types_module.TaskType.PLAYLIST_GENERATION,
        status=types_module.SyncStatus.PENDING,
        params={
            "profile_id": str(profile_id),
            "freshness_target": gen_params.freshness_target,
            "max_tracks": gen_params.max_tracks,
        },
    )
    db.add(task)
    await db.commit()

    arq_redis = request.app.state.arq_redis
    await arq_redis.enqueue_job(
        "generate_playlist",
        str(task.id),
        _job_id=f"generate_playlist:{task.id}",
    )

    logger.info(
        "playlist_generation_triggered",
        profile_id=str(profile_id),
        task_id=str(task.id),
        user_id=str(user_id),
    )

    return {"status": "started", "task_id": str(task.id)}
