"""
This module prepares Navbar of Gateway UI
"""

from nicegui import ui


def highlight_if_route(route: str, current_route: str) -> str:
    """
    Set navbar item highlighted if it is current path
    :param route:
    :param current_route:
    :return:
    """
    if route == current_route:
        return "unelevated color=blue text-white"
    return "flat color=white"


def navbar():
    """
    This method prepares navbar
    :return:
    """

    current_route = ui.context.client.page.path
    with ui.header().classes("items-center justify-between p-0 px-4 no-wrap"):
        with ui.row().classes("items-center"):
            ui.label("Agalar Turizm").classes("text-xl font-bold")
            ui.separator().props("vertical")
            with ui.row():
                ui.button("Query", on_click=lambda: ui.navigate.to("/")).props(highlight_if_route("/", current_route))
                ui.button("Analyze", on_click=lambda: ui.navigate.to("/analyze")).props(
                    highlight_if_route("/analyze", current_route)
                )
