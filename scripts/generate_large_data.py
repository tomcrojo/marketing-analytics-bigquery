#!/usr/bin/env python3
"""Generate large-scale marketing analytics CSV data using Faker.

Usage:
    python scripts/generate_large_data.py [--impressions 5000000] [--clicks-ratio 0.24] [--conversions-ratio 0.29]
    python scripts/generate_large_data.py --sample  # 10K impressions for testing
"""

import argparse
import csv
import os
import random
import uuid
from datetime import datetime, timedelta

from faker import Faker
from tqdm import tqdm

fake = Faker("es_ES")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "data")

CHUNK_SIZE = 100_000
NUM_CAMPAIGNS = 50
NUM_CUSTOMERS = 50_000
IMPRESSION_DATE_START = datetime(2024, 1, 1)
IMPRESSION_DATE_END = datetime(2026, 3, 21)

CHANNELS = ["Google_Ads", "Meta_Ads", "TikTok_Ads", "LinkedIn_Ads", "Email", "SEO"]
CHANNEL_PLATFORM = {
    "Google_Ads": "google",
    "Meta_Ads": "meta",
    "TikTok_Ads": "tiktok",
    "LinkedIn_Ads": "linkedin",
    "Email": "email",
    "SEO": "organic",
}
DEVICES = ["mobile", "desktop", "tablet"]
COUNTRIES = ["ES", "FR", "DE", "PT", "IT", "UK"]
CONVERSION_TYPES = ["purchase", "signup", "lead"]
OBJECTIVES = ["awareness", "consideration", "conversion"]

SPANISH_CAMPAIGN_NAMES = [
    "Verano_Espana_Awareness",
    "Black_Friday_Conversion",
    "Navidad_Remarketing",
    "TikTok_GenZ_Spring",
    "LinkedIn_B2B_Leads",
    "Email_Loyalty_Q4",
    "SEO_Content_Spain",
    "Meta_Retargeting_Q1",
    "Google_Shopping_Summer",
    "TikTok_Viral_Challenge",
    "Primera_Comunion_Promo",
    "Dia_Madre_Campaign",
    "San_Fermin_Regional",
    "Back_to_School_ES",
    "Rebajas_Enero_Meta",
    "Google_Autumn_Collections",
    "B2B_SaaS_LinkedIn",
    "Retargeting_Carritos_Q2",
    "Travel_Summer_Meta",
    "Fitness_New_Year_Email",
    "Food_Delivery_TikTok",
    "Real_Estate_SEO_Pack",
    "Fintech_Acquisition_Google",
    "Luxury_Fashion_Meta",
    "Ecommerce_Flash_Sale",
    "Spanish_Tourism_SEO",
    "Gaming_GenZ_TikTok",
    "Startup_Fundraising_LinkedIn",
    "Subscription_Box_Email",
    "Pet_Care_Remarketing",
    "Organic_Food_Awareness",
    "Home_Decor_Pinterest_ES",
    "Festival_Music_Promo",
    "Language_App_Acquisition",
    "Insurance_B2C_Google",
    "Nursery_School_Meta",
    "Used_Cars_SEO_ES",
    "Smartphone_Launch_TikTok",
    "University_Leads_LinkedIn",
    "Holiday_Gift_Guide_Email",
    "Winter_Sports_Meta",
    "Gastronomy_Tour_SEO",
    "Gym_Membership_Q1",
    "Streaming_Service_Google",
    "Eco_Products_TikTok",
    "Wedding_Season_Meta",
    "Tax_Software_Q1_LinkedIn",
    "Beauty_Spring_Email",
    "Apartment_Rental_SEO",
    "Barcelona_Events_Meta",
]

TARGET_AUDIENCES = [
    "18-45 España",
    "25-55 compradores online",
    "Visitantes web 30d",
    "18-28 España tendencias",
    "Directores marketing España",
    "Clientes activos",
    "Tráfico orgánico",
    "Carritos abandonados",
    "Compradores e-commerce",
    "16-30 creadores contenido",
    "Jóvenes profesionales Madrid",
    "Pymes España 10-50 empleados",
    "Turistas europa destino ES",
    "Padres primer hijo 25-40",
    "Emprendedores digital nomads",
]


def _date_range_days():
    return (IMPRESSION_DATE_END - IMPRESSION_DATE_START).days


def generate_campaigns(data_dir):
    """Write raw_campaigns.csv with 50 campaigns."""
    path = os.path.join(data_dir, "raw_campaigns.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "campaign_id",
                "campaign_name",
                "channel",
                "platform",
                "start_date",
                "end_date",
                "budget_euros",
                "target_audience",
                "objective",
            ]
        )
        range_days = _date_range_days()
        for i in range(1, NUM_CAMPAIGNS + 1):
            cid = f"C{i:03d}"
            name = SPANISH_CAMPAIGN_NAMES[i - 1]
            channel = CHANNELS[i % len(CHANNELS)]
            platform = CHANNEL_PLATFORM[channel]
            start = IMPRESSION_DATE_START + timedelta(
                days=random.randint(0, range_days - 60)
            )
            end = start + timedelta(days=random.randint(30, 180))
            if end > IMPRESSION_DATE_END:
                end = IMPRESSION_DATE_END
            budget = round(random.uniform(500, 50000), 2)
            audience = random.choice(TARGET_AUDIENCES)
            objective = OBJECTIVES[i % len(OBJECTIVES)]
            writer.writerow(
                [
                    cid,
                    name,
                    channel,
                    platform,
                    start.strftime("%Y-%m-%d"),
                    end.strftime("%Y-%m-%d"),
                    budget,
                    audience,
                    objective,
                ]
            )
    return path


def generate_data(
    num_impressions=5_000_000,
    clicks_ratio=0.24,
    conversions_ratio=0.29,
    sample=False,
    seed=42,
):
    rng = random.Random(seed)

    if sample:
        num_impressions = 10_000

    total_clicks = int(num_impressions * clicks_ratio)
    total_conversions = int(total_clicks * conversions_ratio)

    os.makedirs(DATA_DIR, exist_ok=True)

    print(f"=== Marketing Analytics Large-Scale Data Generator ===")
    print(f"  Impressions:  {num_impressions:>12,}")
    print(f"  Clicks:       {total_clicks:>12,}  ({clicks_ratio:.0%} CTR)")
    print(
        f"  Conversions:  {total_conversions:>12,}  ({conversions_ratio:.0%} Conv Rate)"
    )
    print(f"  Customers:    {NUM_CUSTOMERS:>12,}")
    print(f"  Campaigns:    {NUM_CAMPAIGNS:>12,}")
    print()

    # ── Campaigns ──────────────────────────────────────────────
    print("Generating campaigns...")
    cp = generate_campaigns(DATA_DIR)
    print(f"  → {cp}  ({NUM_CAMPAIGNS} rows)\n")

    # ── Impression IDs on disk (index file for later sampling) ─
    imp_id_path = os.path.join(DATA_DIR, "_imp_ids.tmp")
    imp_path = os.path.join(DATA_DIR, "raw_ad_impressions.csv")
    range_days = _date_range_days()

    print("Generating raw_ad_impressions.csv ...")
    with (
        open(imp_path, "w", newline="", encoding="utf-8") as csv_f,
        open(imp_id_path, "w", encoding="utf-8") as id_f,
    ):
        writer = csv.writer(csv_f)
        writer.writerow(
            [
                "impression_id",
                "campaign_id",
                "channel",
                "platform",
                "timestamp",
                "device_type",
                "country",
                "cost_micros",
            ]
        )
        for i in tqdm(range(1, num_impressions + 1), desc="  impressions", unit="rows"):
            imp_id = str(uuid.uuid4())
            cid = f"C{rng.randint(1, NUM_CAMPAIGNS):03d}"
            channel = rng.choice(CHANNELS)
            platform = CHANNEL_PLATFORM[channel]
            ts = IMPRESSION_DATE_START + timedelta(
                seconds=rng.randint(0, range_days * 86400)
            )
            device = rng.choice(DEVICES)
            country = rng.choice(COUNTRIES)
            cost = rng.randint(5000, 500000)

            writer.writerow(
                [
                    imp_id,
                    cid,
                    channel,
                    platform,
                    ts.strftime("%Y-%m-%d %H:%M:%S"),
                    device,
                    country,
                    cost,
                ]
            )
            id_f.write(
                f"{imp_id}\t{cid}\t{channel}\t{platform}\t{device}\t{country}\t{cost}\t{ts.isoformat()}\n"
            )

    print(f"  → {imp_path}  ({num_impressions:,} rows)\n")

    # ── Sample impression IDs for clicks ───────────────────────
    print("Sampling impression indices for clicks ...")
    with open(imp_id_path, "r", encoding="utf-8") as f:
        all_lines = f.readlines()

    click_indices = sorted(rng.sample(range(len(all_lines)), total_clicks))

    # ── Clicks ─────────────────────────────────────────────────
    clk_path = os.path.join(DATA_DIR, "raw_clicks.csv")
    clk_id_path = os.path.join(DATA_DIR, "_clk_ids.tmp")

    print("Generating raw_clicks.csv ...")
    with (
        open(clk_path, "w", newline="", encoding="utf-8") as csv_f,
        open(clk_id_path, "w", encoding="utf-8") as id_f,
    ):
        writer = csv.writer(csv_f)
        writer.writerow(
            [
                "click_id",
                "impression_id",
                "campaign_id",
                "channel",
                "platform",
                "timestamp",
                "device_type",
                "country",
                "cost_micros",
            ]
        )
        for idx in tqdm(click_indices, desc="  clicks", unit="rows"):
            fields = all_lines[idx].rstrip("\n").split("\t")
            imp_id, cid, channel, platform, device, country, cost, imp_ts_str = fields

            clk_id = str(uuid.uuid4())
            imp_ts = datetime.fromisoformat(imp_ts_str)
            clk_ts = imp_ts + timedelta(seconds=rng.randint(1, 3600))

            writer.writerow(
                [
                    clk_id,
                    imp_id,
                    cid,
                    channel,
                    platform,
                    clk_ts.strftime("%Y-%m-%d %H:%M:%S"),
                    device,
                    country,
                    cost,
                ]
            )
            id_f.write(f"{clk_id}\n")

    print(f"  → {clk_path}  ({total_clicks:,} rows)\n")

    # ── Free impression index memory ───────────────────────────
    del all_lines, click_indices

    # ── Sample click IDs for conversions ───────────────────────
    print("Sampling click indices for conversions ...")
    with open(clk_id_path, "r", encoding="utf-8") as f:
        all_clk_lines = f.readlines()

    conv_indices = sorted(rng.sample(range(len(all_clk_lines)), total_conversions))

    # ── Build a quick lookup: click_index → (cid, clk_ts, clk_id) ─
    #    We need campaign_id and timestamp for each selected click.
    #    Re-read clicks CSV for just those rows.
    print("Building conversion context from clicks ...")
    click_context = {}
    with open(clk_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row_idx, row in enumerate(reader):
            if row_idx in set(conv_indices):
                click_context[row_idx] = row

    # ── Conversions ────────────────────────────────────────────
    conv_path = os.path.join(DATA_DIR, "raw_conversions.csv")

    print("Generating raw_conversions.csv ...")
    with open(conv_path, "w", newline="", encoding="utf-8") as csv_f:
        writer = csv.writer(csv_f)
        writer.writerow(
            [
                "conversion_id",
                "click_id",
                "campaign_id",
                "customer_id",
                "timestamp",
                "revenue",
                "conversion_type",
            ]
        )
        cust_ids = [f"CUST-{i:05d}" for i in range(1, NUM_CUSTOMERS + 1)]
        for ci in tqdm(conv_indices, desc="  conversions", unit="rows"):
            row = click_context[ci]
            conv_id = str(uuid.uuid4())
            clk_id = row["click_id"]
            cid = row["campaign_id"]
            cust = rng.choice(cust_ids)
            clk_ts = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S")
            conv_ts = clk_ts + timedelta(minutes=rng.randint(1, 1440))

            conv_type = rng.choice(CONVERSION_TYPES)
            if conv_type == "purchase":
                revenue = round(rng.uniform(5.0, 2000.0), 2)
            elif conv_type == "lead":
                revenue = round(rng.uniform(0.0, 500.0), 2)
            else:
                revenue = 0.0

            writer.writerow(
                [
                    conv_id,
                    clk_id,
                    cid,
                    cust,
                    conv_ts.strftime("%Y-%m-%d %H:%M:%S"),
                    revenue,
                    conv_type,
                ]
            )

    print(f"  → {conv_path}  ({total_conversions:,} rows)\n")

    # ── Cleanup temp files ─────────────────────────────────────
    for tmp in (imp_id_path, clk_id_path):
        if os.path.exists(tmp):
            os.remove(tmp)

    print("=== Generation complete ===")
    print(
        f"  Total rows: {num_impressions + total_clicks + total_conversions + NUM_CAMPAIGNS:,}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Generate large-scale marketing analytics CSV data."
    )
    parser.add_argument(
        "--impressions",
        type=int,
        default=5_000_000,
        help="Number of impression rows (default: 5,000,000)",
    )
    parser.add_argument(
        "--clicks-ratio",
        type=float,
        default=0.24,
        help="Click-through rate as fraction (default: 0.24)",
    )
    parser.add_argument(
        "--conversions-ratio",
        type=float,
        default=0.29,
        help="Conversion rate as fraction (default: 0.29)",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Generate only 10K impressions (for testing)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    args = parser.parse_args()
    generate_data(
        num_impressions=args.impressions,
        clicks_ratio=args.clicks_ratio,
        conversions_ratio=args.conversions_ratio,
        sample=args.sample,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
