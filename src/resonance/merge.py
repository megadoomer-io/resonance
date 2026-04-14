"""Account merge functions for combining duplicate user accounts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async

import resonance.models as models

if TYPE_CHECKING:
    import uuid


@dataclass
class MergeStats:
    """Statistics from an account merge operation."""

    connections_moved: int = 0
    events_moved: int = 0
    artist_relations_moved: int = 0
    artist_relations_skipped: int = 0
    track_relations_moved: int = 0
    track_relations_skipped: int = 0
    sync_tasks_moved: int = 0


async def get_account_summary(
    session: sa_async.AsyncSession,
    user_id: uuid.UUID,
) -> dict[str, int]:
    """Return data counts for a user account.

    Args:
        session: An async SQLAlchemy session.
        user_id: The user whose data to count.

    Returns:
        A dict mapping category names to their counts.
    """
    model_map: list[tuple[str, type[models.Base]]] = [
        ("connections", models.ServiceConnection),
        ("listening_events", models.ListeningEvent),
        ("artist_relations", models.UserArtistRelation),
        ("track_relations", models.UserTrackRelation),
        ("sync_tasks", models.Task),
    ]

    counts: dict[str, int] = {}
    for key, model_cls in model_map:
        stmt = (
            sa.select(sa.func.count())
            .select_from(model_cls)
            .where(model_cls.user_id == user_id)  # type: ignore[attr-defined]
        )
        result = await session.execute(stmt)
        counts[key] = result.scalar_one()

    return counts


async def merge_accounts(
    session: sa_async.AsyncSession,
    target_user_id: uuid.UUID,
    source_user_id: uuid.UUID,
) -> MergeStats:
    """Merge all data from source_user into target_user.

    Caller is responsible for committing the transaction.

    Args:
        session: An async SQLAlchemy session (within a transaction).
        target_user_id: The user account that will receive data.
        source_user_id: The user account whose data will be moved and then deleted.

    Returns:
        Statistics describing what was moved, skipped, and deleted.
    """
    stats = MergeStats()

    # 1. Move ServiceConnections
    cursor = cast(
        "sa.CursorResult[tuple[()]]",
        await session.execute(
            sa.update(models.ServiceConnection)
            .where(models.ServiceConnection.user_id == source_user_id)
            .values(user_id=target_user_id)
        ),
    )
    stats.connections_moved = cursor.rowcount

    # 2. Move ListeningEvents
    cursor = cast(
        "sa.CursorResult[tuple[()]]",
        await session.execute(
            sa.update(models.ListeningEvent)
            .where(models.ListeningEvent.user_id == source_user_id)
            .values(user_id=target_user_id)
        ),
    )
    stats.events_moved = cursor.rowcount

    # 3. Move UserArtistRelations (duplicate-aware)
    (
        stats.artist_relations_moved,
        stats.artist_relations_skipped,
    ) = await _merge_artist_relations(session, target_user_id, source_user_id)

    # 4. Move UserTrackRelations (duplicate-aware)
    (
        stats.track_relations_moved,
        stats.track_relations_skipped,
    ) = await _merge_track_relations(session, target_user_id, source_user_id)

    # 5. Move SyncTasks
    cursor = cast(
        "sa.CursorResult[tuple[()]]",
        await session.execute(
            sa.update(models.Task)
            .where(models.Task.user_id == source_user_id)
            .values(user_id=target_user_id)
        ),
    )
    stats.sync_tasks_moved = cursor.rowcount

    # 6. Delete source user
    await session.execute(
        sa.delete(models.User).where(models.User.id == source_user_id)
    )

    return stats


async def _merge_artist_relations(
    session: sa_async.AsyncSession,
    target_user_id: uuid.UUID,
    source_user_id: uuid.UUID,
) -> tuple[int, int]:
    """Move artist relations, skipping duplicates.

    Returns:
        A tuple of (moved_count, skipped_count).
    """
    source_result = await session.execute(
        sa.select(models.UserArtistRelation).where(
            models.UserArtistRelation.user_id == source_user_id
        )
    )
    source_rels = source_result.scalars().all()

    target_result = await session.execute(
        sa.select(models.UserArtistRelation).where(
            models.UserArtistRelation.user_id == target_user_id
        )
    )
    target_rels = target_result.scalars().all()

    target_keys: set[tuple[uuid.UUID, str, str]] = {
        (r.artist_id, r.relation_type, r.source_service) for r in target_rels
    }

    moved = 0
    skipped = 0
    for rel in source_rels:
        key = (rel.artist_id, rel.relation_type, rel.source_service)
        if key in target_keys:
            await session.execute(
                sa.delete(models.UserArtistRelation).where(
                    models.UserArtistRelation.id == rel.id
                )
            )
            skipped += 1
        else:
            await session.execute(
                sa.update(models.UserArtistRelation)
                .where(models.UserArtistRelation.id == rel.id)
                .values(user_id=target_user_id)
            )
            moved += 1

    return moved, skipped


async def _merge_track_relations(
    session: sa_async.AsyncSession,
    target_user_id: uuid.UUID,
    source_user_id: uuid.UUID,
) -> tuple[int, int]:
    """Move track relations, skipping duplicates.

    Returns:
        A tuple of (moved_count, skipped_count).
    """
    source_result = await session.execute(
        sa.select(models.UserTrackRelation).where(
            models.UserTrackRelation.user_id == source_user_id
        )
    )
    source_rels = source_result.scalars().all()

    target_result = await session.execute(
        sa.select(models.UserTrackRelation).where(
            models.UserTrackRelation.user_id == target_user_id
        )
    )
    target_rels = target_result.scalars().all()

    target_keys: set[tuple[uuid.UUID, str, str]] = {
        (r.track_id, r.relation_type, r.source_service) for r in target_rels
    }

    moved = 0
    skipped = 0
    for rel in source_rels:
        key = (rel.track_id, rel.relation_type, rel.source_service)
        if key in target_keys:
            await session.execute(
                sa.delete(models.UserTrackRelation).where(
                    models.UserTrackRelation.id == rel.id
                )
            )
            skipped += 1
        else:
            await session.execute(
                sa.update(models.UserTrackRelation)
                .where(models.UserTrackRelation.id == rel.id)
                .values(user_id=target_user_id)
            )
            moved += 1

    return moved, skipped
