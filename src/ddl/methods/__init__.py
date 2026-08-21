from src.ddl.ddl_utils import DDLMethod


def get_queries(cfg) -> list[str]:
    """Generate SQL queries based on the provided configuration."""
    method = cfg.method
    if method == DDLMethod.CREATE:
        from src.ddl.methods.create import run as create_run

        return create_run(cfg.params)
    else:
        raise ValueError(f'Unsupported method: {method}')
