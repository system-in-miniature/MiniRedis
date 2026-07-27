from miniredis.runtime import MiniRedis, _RuntimeTestHooks


class TestMiniRedis(MiniRedis):
    pass


async def open_test_runtime(
    *,
    clock=None,
    scheduler=None,
    aof_appender=None,
    config=None,
) -> TestMiniRedis:
    runtime = TestMiniRedis._for_test(
        config=config,
        clock=clock,
        scheduler=scheduler,
        test_hooks=_RuntimeTestHooks(aof_appender=aof_appender),
    )
    await runtime.start()
    return runtime
