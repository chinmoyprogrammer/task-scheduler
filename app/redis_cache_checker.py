import os
import asyncio
import redis.asyncio as redis


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)
CACHE_PREFIX = os.getenv("CACHE_PREFIX", "hr_cache")
REDIS_KEY_PREFIX = f"{CACHE_PREFIX}:" if CACHE_PREFIX else ""


REQUIRED_CACHE_KEYS = [
    "all_employees",
    "all_employee_short_desc",
    "all_business_settings",
    "all_divisions",
    "all_districts",
    "all_upazilas",
    "all_leave_heads",
    "all_religions",
    "all_genders",
    "all_companies",
    "all_branches",
    "all_departments",
    "all_sections",
    "all_subsections",
    "all_designation_levels",
    "all_designations",
    "all_employee_types",
    "all_bank_names",
    "all_bank_branches",
    "all_mfs_list",
    "all_pf_policies",
    "all_ot_policies",
    "all_occupations",
]


def _prefixed(key: str) -> str:
    return f"{REDIS_KEY_PREFIX}{key}"


async def get_missing_cache_keys(required_keys: list = None) -> list:
    keys = required_keys or REQUIRED_CACHE_KEYS
    client = None
    missing = []
    try:
        client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=False,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        for key in keys:
            exists = await client.exists(_prefixed(key))
            if not exists:
                missing.append(key)
    except Exception as e:
        print(f"[redis_cache_checker] Redis check failed, treating all keys as missing: {e}")
        missing = list(keys)
    finally:
        if client is not None:
            try:
                await client.close()
            except Exception:
                pass
    return missing
