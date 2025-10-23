traffic_data = response.json()

    raw_ts = traffic_data.get('sourceUpdated')


    if raw_ts:
        sanitized = raw_ts.replace(':', '-').replace(' ', '_')
        sanitized = ''.join(c for c in sanitized if c.isalnum() or c in ('-', '_'))
    else:
        from datetime import datetime
        sanitized = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

    OUT_DIR = f"data/raw/{sanitized}"
    import os
    os.makedirs(OUT_DIR, exist_ok=True)

    out_file = os.path.join(OUT_DIR, 'hanoi_traffic_data.json')

    with open(out_file, 'w', encoding='utf-8') as f:
        f = json.dump(traffic_data, f, ensure_ascii=False, indent=2)

    print(f"Saved raw traffic data to: {out_file}")