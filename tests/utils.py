import asyncio

from crate.operator.constants import BACKOFF_TIME


async def assert_wait_for(
    condition, coro_func, *args, err_msg="", timeout=BACKOFF_TIME, **kwargs
):
    ret_val = await coro_func(*args, **kwargs)
    duration = 0.0
    base = 2.0
    count = 0
    while ret_val is not condition:
        count += 1
        delay = base ** (count * 0.5)
        await asyncio.sleep(delay)
        ret_val = await coro_func(*args, **kwargs)
        if ret_val is not condition and duration > timeout:
            break
        else:
            duration += delay
    assert ret_val is condition, err_msg


async def does_namespace_exist(core, namespace: str) -> bool:
    namespaces = await core.list_namespace()
    return namespace in (ns.metadata.name for ns in namespaces.items)
