import os

import pandas as pd

import streamlit as st

from sqlalchemy import create_engine
from sqlalchemy import text as sql_text

from streamlit_autorefresh import (  # type: ignore
    st_autorefresh
)


DB_HOST = os.getenv(
    "DB_HOST",
    "postgres"
)

DB_NAME = os.getenv(
    "DB_NAME",
    "traffic_dwh"
)

DB_USER = os.getenv(
    "DB_USER",
    "airflow"
)

DB_PASSWORD = os.getenv(
    "DB_PASSWORD",
    "airflow_password"
)

DB_PORT = int(
    os.getenv(
        "DB_PORT",
        "5432"
    )
)


st.set_page_config(

    page_title=
        "Traffic Monitoring",

    page_icon=
        "🚦",

    layout=
        "wide"

)


st_autorefresh(

    interval=5000,

    key=
        "traffic_dashboard_refresh"

)


def get_engine():

    url = (
        f"postgresql+psycopg2://"
        f"{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}"
        f"/{DB_NAME}"
    )

    return create_engine(
        url,
        connect_args={
            "connect_timeout": 5,
        },
        pool_pre_ping=True,
    )


def query_df(
    query
):

    engine = get_engine()

    with engine.connect() as conn:

        return pd.read_sql_query(

            sql_text(query),

            conn

        )


st.title(
    "🚦 Traffic Monitoring Dashboard"
)


st.caption(

    "Real-Time + Batch Hybrid "
    "Data Engineering Pipeline"

)


try:

    # ==========================================================
    # REAL-TIME METRICS
    # ==========================================================

    metrics = query_df(

        """

        SELECT

            camera_id,
            location,
            latitude,
            longitude,
            window_seconds,
            vehicles_in_window,
            avg_speed,
            speeding_violations,
            congestion_level,
            window_end,
            updated_at

        FROM realtime_traffic_metrics

        ORDER BY camera_id;

        """

    )


    # ==========================================================
    # RECENT EVENTS
    # ==========================================================

    recent = query_df(

        """

        SELECT

            event_timestamp,
            camera_id,
            location,
            vehicle_type,
            speed_kmh,
            is_speeding

        FROM realtime_traffic_events

        ORDER BY event_timestamp DESC

        LIMIT 100;

        """

    )


    # ==========================================================
    # HISTORICAL BATCH DATA
    # ==========================================================

    historical = query_df(

        """

        SELECT

            batch_id,
            camera_id,
            location,
            total_vehicles,
            avg_speed,
            speeding_violations,
            congestion_level,
            window_start,
            window_end,
            processed_at

        FROM hourly_traffic_summary

        ORDER BY processed_at DESC

        LIMIT 100;

        """

    )


    total_live = (

        int(
            metrics[
                "vehicles_in_window"
            ].sum()
        )

        if not metrics.empty

        else 0

    )


    total_speeding = (

        int(
            metrics[
                "speeding_violations"
            ].sum()
        )

        if not metrics.empty

        else 0

    )


    avg_live_speed = (

        round(

            float(
                metrics[
                    "avg_speed"
                ].mean()
            ),

            2

        )

        if not metrics.empty

        else 0

    )


    high_cameras = (

        int(

            (
                metrics[
                    "congestion_level"
                ]
                == "HIGH"
            ).sum()

        )

        if not metrics.empty

        else 0

    )


    # ==========================================================
    # KPI ROW
    # ==========================================================

    c1, c2, c3, c4 = st.columns(4)


    c1.metric(

        "Vehicles in Live Windows",

        total_live

    )


    c2.metric(

        "Speeding Violations",

        total_speeding

    )


    c3.metric(

        "Average Live Speed",

        f"{avg_live_speed} km/h"

    )


    c4.metric(

        "High Congestion Cameras",

        high_cameras

    )


    if not metrics.empty:

        latest_update = (
            metrics[
                "updated_at"
            ].max()
        )

        st.caption(

            "Last real-time metric update: "
            f"{latest_update}"

        )


    st.divider()


    # ==========================================================
    # LIVE CAMERA TABLE
    # ==========================================================

    st.subheader(
        "Live Traffic by Camera"
    )


    if metrics.empty:

        st.info(

            "Waiting for real-time "
            "traffic events... "
            "Start the simulator."

        )

    else:

        st.dataframe(

            metrics[

                [
                    "camera_id",
                    "location",
                    "vehicles_in_window",
                    "avg_speed",
                    "speeding_violations",
                    "congestion_level",
                    "window_end",
                ]

            ],

            use_container_width=True,

            hide_index=True,

        )


        # ======================================================
        # MAP
        # ======================================================

        map_data = metrics.dropna(

            subset=[
                "latitude",
                "longitude"
            ]

        ).copy()


        if not map_data.empty:

            st.subheader(
                "Live Camera Map"
            )


            st.map(

                map_data.rename(

                    columns={

                        "latitude":
                            "lat",

                        "longitude":
                            "lon",

                    }

                )[

                    [
                        "lat",
                        "lon"
                    ]

                ]

            )


    st.divider()


    # ==========================================================
    # EVENT + BATCH TABLES
    # ==========================================================

    left, right = st.columns(2)


    with left:

        st.subheader(
            "Recent Events"
        )


        st.dataframe(

            recent,

            use_container_width=True,

            hide_index=True

        )


    with right:

        st.subheader(
            "Historical Batch Results"
        )


        st.dataframe(

            historical,

            use_container_width=True,

            hide_index=True

        )


except Exception as exc:

    st.error(

        "Dashboard cannot connect "
        "to PostgreSQL yet."

    )


    st.code(
        str(exc)
    )


    st.info(

        "Make sure Docker services "
        "are running and the database "
        "initialization completed."

    )