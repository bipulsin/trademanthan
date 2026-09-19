# MCX Date Wise reports (Kosmic Tarang checksum)

Drop MCX **Date Wise** HTML-as-`.xls` (or `.html`) files here. They are not committed.

**Local:** `/Users/bipulsahay/TradeManthan/data/mcx_bhavcopy/datewise/`  
**Production container:** `/app/data/mcx_bhavcopy/datewise/`  
Copy onto paperclip then into the app container:

```
scp file.xls paperclip:/tmp/mcx_datewise/
./scripts/paperclip-ssh.sh 'cd /home/ubuntu/twcto && docker compose exec -T app mkdir -p /app/data/mcx_bhavcopy/datewise'
./scripts/paperclip-ssh.sh 'cd /home/ubuntu/twcto && docker compose cp /tmp/mcx_datewise/. app:/app/data/mcx_bhavcopy/datewise/'
```

Checksum: Date Wise **Traded Contract (Lots)** per date vs `SUM(volume_lots)` of imported Bhavcopy across **all expiries** for that commodity+date. Differences **above 1%** are flagged.

API: `GET /api/tarang/bhavcopy/checksum/scan` (also runs as part of EOD backtest).
