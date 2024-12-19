def update_diff_q():
    return f"""UPDATE debit_and_p as dap_upd
    SET diff_q = dap.sum_q_inj - dap.sum_q_prod
FROM debit_and_p as dap
WHERE dap_upd.field = dap.field
    AND dap_upd.date_origin = dap.date_origin;"""