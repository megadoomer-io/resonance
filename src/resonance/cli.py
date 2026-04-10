"""CLI commands for Resonance administration."""

import asyncio
import sys
import uuid

import sqlalchemy as sa

import resonance.config as config_module
import resonance.database as database_module
import resonance.models.user as user_models
import resonance.types as types_module


async def _set_role(user_id_str: str, role_str: str) -> None:
    """Set a user's role directly in the database."""
    settings = config_module.Settings()
    engine = database_module.create_async_engine(settings)
    session_factory = database_module.create_session_factory(engine)

    try:
        user_id = uuid.UUID(user_id_str)
    except ValueError:
        print(f"Error: Invalid UUID: {user_id_str}")
        sys.exit(1)

    try:
        role = types_module.UserRole(role_str)
    except ValueError:
        valid = ", ".join(r.value for r in types_module.UserRole)
        print(f"Error: Invalid role '{role_str}'. Valid roles: {valid}")
        sys.exit(1)

    async with session_factory() as db:
        result = await db.execute(
            sa.select(user_models.User).where(user_models.User.id == user_id)
        )
        user = result.scalar_one_or_none()
        if user is None:
            print(f"Error: No user found with ID {user_id}")
            sys.exit(1)

        old_role = user.role
        user.role = role
        await db.commit()
        print(f"Updated {user.display_name}: {old_role.value} → {role.value}")

    await engine.dispose()


def set_role() -> None:
    """Entry point for `resonance-set-role <user_id> <role>`."""
    if len(sys.argv) != 3:
        print("Usage: resonance-set-role <user_id> <role>")
        print(f"  Roles: {', '.join(r.value for r in types_module.UserRole)}")
        sys.exit(1)

    asyncio.run(_set_role(sys.argv[1], sys.argv[2]))
