import asyncio, sys, os
sys.path.insert(0, os.path.dirname(__file__))
from extract_gold_structured import run_gold_structured

# All remaining 2025-2026 months
months = [
    '2025-02','2025-03','2025-04','2025-05','2025-06',
    '2025-07','2025-08','2025-09','2025-10','2025-11','2025-12',
    '2026-01','2026-02','2026-03','2026-04','2026-05','2026-06',
]

async def main():
    for ym in months:
        print(f'START {ym}', flush=True)
        await run_gold_structured(start_date=ym, end_date=ym, workers=30)

asyncio.run(main())
