from sqlmesh import macro

@macro()
def snowflake_only(evaluator):
    if evaluator.gateway == 'snowflake':
        return True
    else:
        return False