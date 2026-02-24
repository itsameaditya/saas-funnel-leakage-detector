"""
Synthetic SaaS Funnel Dataset Generator (Medium: 20k users)
Generates realistic event logs + subscriptions with embedded business patterns:
- Channel quality differences
- Multi-session behavior across days
- Activation strongly drives trial + subscription
- Mobile + certain countries have higher payment failures
- More realistic event volume (~300k-500k)

Outputs:
- data/users.csv
- data/events.csv
- data/subscriptions.csv
"""

import os
import random
from datetime import datetime, timedelta
import uuid

import numpy as np
import pandas as pd
from faker import Faker
from tqdm import tqdm

# -----------------------
# CONFIG
# -----------------------
NUM_USERS = 20000
SEED = 42
DAYS_LOOKBACK = 90

TARGET_EVENT_VOLUME_HINT = "Expected ~300k–500k events (varies with randomness)"

fake = Faker()
random.seed(SEED)
np.random.seed(SEED)

# -----------------------
# DISTRIBUTIONS
# -----------------------
CHANNELS = {
    "organic": 0.30,
    "paid_search": 0.20,
    "paid_social": 0.25,
    "partner": 0.15,
    "referral": 0.10,
}

DEVICES = {"web": 0.70, "mobile": 0.30}

COUNTRIES = {
    "US": 0.40,
    "IN": 0.25,
    "UK": 0.15,
    "CA": 0.10,
    "AU": 0.10,
}

COMPANY_SIZES = {
    "solo": 0.35,
    "2-10": 0.30,
    "11-50": 0.20,
    "51-200": 0.15,
}

PERSONAS = ["maker", "startup_ops", "analyst", "agency"]

# Noise events that happen during browsing / research
BROWSING_EVENTS = [
    "landing_page_view",
    "pricing_view",
    "case_study_view",
    "docs_view",
    "faq_view",
]

# Product usage / engagement noise events
PRODUCT_EVENTS = [
    "dashboard_view",
    "settings_view",
    "project_view",
    "task_create",
    "task_update",
    "search",
]

INTEGRATIONS = ["slack", "google_drive", "github"]

PAYMENT_ERROR_CODES = ["PMT_001", "PMT_002", "PMT_003", "PMT_NET", "PMT_3DS"]


# -----------------------
# HELPERS
# -----------------------
def weighted_choice(d: dict):
    return np.random.choice(list(d.keys()), p=list(d.values()))


def random_timestamp_within_last_n_days(n_days: int):
    now = datetime.now()
    delta = timedelta(days=random.randint(0, n_days), minutes=random.randint(0, 1440))
    return now - delta


def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def generate_user(user_id: int):
    signup_ts = random_timestamp_within_last_n_days(DAYS_LOOKBACK)

    return {
        "user_id": user_id,
        "signup_ts": signup_ts,
        "acquisition_channel": weighted_choice(CHANNELS),
        "device": weighted_choice(DEVICES),
        "country": weighted_choice(COUNTRIES),
        "company_size": weighted_choice(COMPANY_SIZES),
        "persona": random.choice(PERSONAS),
        "is_b2b_email": bool(random.random() < 0.65),
    }


def onboarding_start_probability(channel: str) -> float:
    # Paid social is often lower intent; referral/organic higher intent
    base = {
        "organic": 0.88,
        "referral": 0.90,
        "paid_search": 0.85,
        "partner": 0.82,
        "paid_social": 0.78,
    }[channel]
    return base


def activation_probability(
    channel: str, device: str, persona: str, company_size: str
) -> float:
    # Base by channel (quality)
    base = {
        "organic": 0.52,
        "referral": 0.58,
        "paid_search": 0.42,
        "partner": 0.38,
        "paid_social": 0.28,
    }[channel]

    # Mobile friction
    if device == "mobile":
        base -= 0.05

    # Persona effect: makers/agency often activate faster
    if persona in ["maker", "agency"]:
        base += 0.03
    elif persona == "analyst":
        base += 0.01

    # Larger teams may have more setup overhead but higher eventual value
    if company_size in ["51-200"]:
        base -= 0.02

    return clamp01(base)


def value_moment_probability(persona: str) -> float:
    # Value moment = invites or integration; varies by persona
    return {
        "maker": 0.35,
        "startup_ops": 0.45,
        "analyst": 0.30,
        "agency": 0.50,
    }[persona]


def trial_probability(activated: bool, channel: str) -> float:
    # Activated users are much more likely to start trial
    base = 0.62 if activated else 0.07
    # Channels influence intent to pay
    if channel in ["referral", "organic"]:
        base += 0.03
    if channel == "paid_social":
        base -= 0.02
    return clamp01(base)


def payment_failure_probability(device: str, country: str) -> float:
    # Mobile higher; India slightly higher (card issues / 3DS / network)
    base = 0.045 if device == "web" else 0.12
    if country == "IN":
        base += 0.04
    return clamp01(base)


def trial_to_paid_probability(
    activated: bool, channel: str, company_size: str
) -> float:
    # This is the big driver for realistic paid conversion (keep it modest)
    base = 0.42 if activated else 0.06

    # Channel quality effects
    if channel == "referral":
        base += 0.05
    elif channel == "organic":
        base += 0.03
    elif channel == "paid_social":
        base -= 0.05

    # Larger companies more likely to pay
    if company_size in ["11-50", "51-200"]:
        base += 0.04

    return clamp01(base)


def pick_plan(company_size: str):
    if company_size == "solo":
        return "starter", 29
    elif company_size in ["2-10", "11-50"]:
        return "pro", 79
    else:
        return "business", 199


def churn_probability(plan: str) -> float:
    return {"starter": 0.30, "pro": 0.18, "business": 0.10}[plan]


def add_event(
    events_list,
    user_id: int,
    event_ts: datetime,
    event_name: str,
    session_id: str,
    page: str = None,
    referrer: str = None,
    props: dict | None = None,
):
    events_list.append(
        {
            "event_id": f"evt_{uuid.uuid4().hex}",
            "user_id": user_id,
            "event_ts": event_ts,
            "session_id": session_id,
            "event_name": event_name,
            "page": page or "",
            "referrer": referrer or "",
            "event_properties": "{}" if not props else str(props).replace("'", '"'),
        }
    )


# -----------------------
# GENERATION
# -----------------------
def generate_data():
    users = []
    events = []
    subscriptions = []

    for user_id in tqdm(range(1, NUM_USERS + 1)):
        user = generate_user(user_id)
        users.append(user)

        # Simulate number of sessions (most users 1–3, some 0 after signup, some heavy)
        # Using a skewed distribution
        sessions = np.random.choice([1, 2, 3, 4, 5], p=[0.45, 0.28, 0.16, 0.08, 0.03])

        # Session start baseline
        t0 = user["signup_ts"]

        activated = False
        value_moment = False
        trial_started = False
        subscribed = False
        subscription_start_ts = None

        # --- Session loop ---
        for s in range(sessions):
            # each session starts later (minutes to a few days)
            session_gap_hours = int(
                np.random.choice(
                    [0, 1, 3, 8, 24, 48], p=[0.20, 0.20, 0.20, 0.15, 0.18, 0.07]
                )
            )
            session_start = (
                t0
                + timedelta(hours=session_gap_hours)
                + timedelta(minutes=random.randint(0, 120))
            )
            session_id = f"sess_{uuid.uuid4().hex[:10]}"

            # referrer hint based on channel
            if user["acquisition_channel"] in ["paid_social"]:
                referrer = np.random.choice(
                    ["linkedin", "instagram", "facebook"], p=[0.55, 0.25, 0.20]
                )
            elif user["acquisition_channel"] in ["paid_search", "organic"]:
                referrer = "google"
            elif user["acquisition_channel"] == "partner":
                referrer = "partner_site"
            else:
                referrer = "direct"

            t = session_start

            # Browsing noise (1–4 events)
            browse_count = int(
                np.random.choice([1, 2, 3, 4], p=[0.35, 0.35, 0.20, 0.10])
            )
            for _ in range(browse_count):
                ev = random.choice(BROWSING_EVENTS)
                page_map = {
                    "landing_page_view": "/",
                    "pricing_view": "/pricing",
                    "case_study_view": "/customers",
                    "docs_view": "/docs",
                    "faq_view": "/faq",
                }
                add_event(
                    events,
                    user_id,
                    t,
                    ev,
                    session_id,
                    page=page_map.get(ev, ""),
                    referrer=referrer,
                )
                t += timedelta(minutes=random.randint(1, 20))

            # Signup happens once (first session)
            if s == 0:
                add_event(
                    events,
                    user_id,
                    t,
                    "signup",
                    session_id,
                    page="/signup",
                    referrer=referrer,
                )
                t += timedelta(minutes=random.randint(1, 30))

                # email verify not guaranteed
                if random.random() < 0.78:
                    add_event(
                        events, user_id, t, "email_verified", session_id, page="/verify"
                    )
                    t += timedelta(minutes=random.randint(1, 30))

                # onboarding start (not guaranteed)
                if random.random() < onboarding_start_probability(
                    user["acquisition_channel"]
                ):
                    add_event(
                        events,
                        user_id,
                        t,
                        "onboarding_start",
                        session_id,
                        page="/onboarding",
                    )
                    t += timedelta(minutes=random.randint(2, 60))

                    # onboarding complete sometimes
                    if random.random() < 0.70:
                        add_event(
                            events,
                            user_id,
                            t,
                            "onboarding_complete",
                            session_id,
                            page="/onboarding/done",
                        )
                        t += timedelta(minutes=random.randint(2, 30))

            # Try to activate if not already activated
            if not activated:
                if random.random() < activation_probability(
                    user["acquisition_channel"],
                    user["device"],
                    user["persona"],
                    user["company_size"],
                ):
                    add_event(
                        events,
                        user_id,
                        t,
                        "create_first_project",
                        session_id,
                        page="/app/new_project",
                    )
                    t += timedelta(minutes=random.randint(2, 40))
                    activated = True

                    # Some immediate product events after activation
                    for _ in range(
                        int(np.random.choice([1, 2, 3, 4], p=[0.20, 0.35, 0.30, 0.15]))
                    ):
                        pe = random.choice(PRODUCT_EVENTS)
                        add_event(events, user_id, t, pe, session_id, page="/app")
                        t += timedelta(minutes=random.randint(1, 25))

                    # Value moment (invite or integration) sometimes follows
                    if random.random() < value_moment_probability(user["persona"]):
                        if random.random() < 0.55:
                            add_event(
                                events,
                                user_id,
                                t,
                                "invite_teammate",
                                session_id,
                                page="/app/invite",
                                props={
                                    "invite_count": int(
                                        np.random.choice(
                                            [1, 2, 3], p=[0.55, 0.30, 0.15]
                                        )
                                    )
                                },
                            )
                        else:
                            integ = random.choice(INTEGRATIONS)
                            add_event(
                                events,
                                user_id,
                                t,
                                "connect_integration",
                                session_id,
                                page="/app/integrations",
                                props={"integration": integ},
                            )
                        t += timedelta(minutes=random.randint(2, 45))
                        value_moment = True

            # Trial start attempt (once)
            if (not trial_started) and random.random() < trial_probability(
                activated, user["acquisition_channel"]
            ):
                add_event(
                    events, user_id, t, "trial_start", session_id, page="/app/billing"
                )
                t += timedelta(minutes=random.randint(1, 30))
                trial_started = True

                # Checkout behavior: retries possible
                checkout_attempts = int(
                    np.random.choice([1, 2, 3], p=[0.70, 0.22, 0.08])
                )
                for attempt in range(checkout_attempts):
                    add_event(
                        events,
                        user_id,
                        t,
                        "checkout_start",
                        session_id,
                        page="/checkout",
                        props={"attempt": attempt + 1},
                    )
                    t += timedelta(minutes=random.randint(1, 10))

                    if random.random() < payment_failure_probability(
                        user["device"], user["country"]
                    ):
                        add_event(
                            events,
                            user_id,
                            t,
                            "payment_failed",
                            session_id,
                            page="/checkout",
                            props={"error_code": random.choice(PAYMENT_ERROR_CODES)},
                        )
                        t += timedelta(minutes=random.randint(3, 25))
                        # many users stop after a failure; some continue retrying
                        if random.random() < 0.65:
                            break
                    else:
                        # payment success doesn't guarantee subscription; decision probability happens here
                        if (
                            not subscribed
                        ) and random.random() < trial_to_paid_probability(
                            activated, user["acquisition_channel"], user["company_size"]
                        ):
                            add_event(
                                events,
                                user_id,
                                t,
                                "subscription_created",
                                session_id,
                                page="/checkout/success",
                            )
                            subscribed = True
                            subscription_start_ts = t
                        t += timedelta(minutes=random.randint(1, 15))
                        break  # exit attempts after success flow

            # Retention proxy events (if activated / subscribed, more likely)
            # Create events on later sessions too
            if activated:
                if random.random() < (0.40 if not subscribed else 0.55):
                    add_event(
                        events, user_id, t, "dashboard_view", session_id, page="/app"
                    )
                    t += timedelta(minutes=random.randint(1, 20))

        # --- After sessions, create subscription record + churn and retention markers ---
        if subscribed and subscription_start_ts is not None:
            plan, mrr = pick_plan(user["company_size"])

            churn_ts = None
            if random.random() < churn_probability(plan):
                churn_ts = subscription_start_ts + timedelta(
                    days=random.randint(30, 75)
                )
                # churn event
                add_event(
                    events,
                    user_id,
                    churn_ts,
                    "cancel_subscription",
                    f"sess_{uuid.uuid4().hex[:10]}",
                    page="/app/billing",
                    props={
                        "reason": random.choice(
                            ["price", "low_value", "unknown", "competitor"]
                        )
                    },
                )

            subscriptions.append(
                {
                    "user_id": user_id,
                    "plan": plan,
                    "mrr": float(mrr),
                    "trial_start_ts": "",  # optional (you can compute from events later)
                    "subscription_start_ts": subscription_start_ts,
                    "churn_ts": churn_ts,
                }
            )

            # retention proxies: more likely if not churned early
            if churn_ts is None or (churn_ts - subscription_start_ts).days >= 7:
                if random.random() < 0.60:
                    add_event(
                        events,
                        user_id,
                        subscription_start_ts + timedelta(days=7),
                        "active_day_7",
                        f"sess_{uuid.uuid4().hex[:10]}",
                        page="/app",
                    )
            if churn_ts is None or (churn_ts - subscription_start_ts).days >= 30:
                if random.random() < 0.45:
                    add_event(
                        events,
                        user_id,
                        subscription_start_ts + timedelta(days=30),
                        "active_day_30",
                        f"sess_{uuid.uuid4().hex[:10]}",
                        page="/app",
                    )

        else:
            # even non-subscribers can have some retention events (activated users exploring)
            if activated and random.random() < 0.18:
                t_ret = user["signup_ts"] + timedelta(
                    days=7, minutes=random.randint(0, 600)
                )
                add_event(
                    events,
                    user_id,
                    t_ret,
                    "active_day_7",
                    f"sess_{uuid.uuid4().hex[:10]}",
                    page="/app",
                )

    users_df = pd.DataFrame(users)
    events_df = pd.DataFrame(events)
    subs_df = pd.DataFrame(subscriptions)

    # Sort events for readability
    events_df["event_ts"] = pd.to_datetime(events_df["event_ts"])
    events_df = events_df.sort_values(["user_id", "event_ts"]).reset_index(drop=True)

    users_df["signup_ts"] = pd.to_datetime(users_df["signup_ts"])
    subs_df["subscription_start_ts"] = pd.to_datetime(
        subs_df["subscription_start_ts"], errors="coerce"
    )
    subs_df["churn_ts"] = pd.to_datetime(subs_df["churn_ts"], errors="coerce")

    os.makedirs("data", exist_ok=True)
    users_df.to_csv("data/users.csv", index=False)
    events_df.to_csv("data/events.csv", index=False)
    subs_df.to_csv("data/subscriptions.csv", index=False)

    print("\nDataset Generated Successfully!")
    print(f"Users: {len(users_df)}")
    print(f"Events: {len(events_df)}  ({TARGET_EVENT_VOLUME_HINT})")
    print(f"Subscriptions: {len(subs_df)}  (target ~8–15% of users)")
    print(f"Paid conversion rate: {len(subs_df) / len(users_df):.2%}")


if __name__ == "__main__":
    generate_data()
