"""Operator sign-in for the control tower. Viewing needs no account; acting on a case or an incident does.

The signed-in operator lives in the Streamlit session only. Every action re-checks the role in the database layer
(``signalflow.security.authorize``) and is written to ``ops.audit_log``, so the UI is never the only gate.
"""

from __future__ import annotations

import streamlit as st

from ..db import connect
from ..security import Operator, authenticate


def current() -> Operator | None:
    op = st.session_state.get("operator")
    return op if isinstance(op, Operator) else None


def sidebar_panel() -> None:
    op = current()
    if op is not None:
        st.caption(f"Signed in as **{op.display_name}** · {op.role}")
        if st.button("Sign out", key="sign_out"):
            st.session_state.pop("operator", None)
            st.rerun()
        return
    with st.expander("Operator sign-in"), st.form("sign_in", clear_on_submit=True):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        if st.form_submit_button("Sign in"):
            with connect() as conn:
                found = authenticate(conn, username.strip(), password)
            if found is None:
                st.error("Unknown user, wrong password or disabled account.")
            else:
                st.session_state["operator"] = found
                st.rerun()
    st.caption("Accounts: `signalflow operators add <name> --role analyst`")
