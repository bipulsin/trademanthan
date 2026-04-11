# Cursor Agent Access to EC2 / Scan Logs

The Cursor agent runs in Cursor’s cloud, so it cannot reach your EC2 by default (security group or private network). You can fix this in two ways.

---

## Option 1: Use the API (recommended, no SSH)

Your backend already exposes scan logs over HTTP. If the app is reachable from the internet (e.g. `https://your-domain.com` or `http://EC2_IP:8000`), the Cursor agent can use it and **no SSH or security group changes** are needed.

### Endpoint

- **URL:** `GET /scan/logs`
- **Query params:**
  - `lines` – number of lines (1–10000), default 100
  - `log_type` – `smart_future_algo` (default), legacy `scan_st1_algo`, or `trademanthan`
  - `grep` – optional substring to filter lines (case-insensitive). Use this so the agent gets only relevant lines.

### Examples (replace with your base URL)

```text
# Last 5000 lines containing OPTION_CHAIN_FAIL (e.g. option chain failures)
GET https://your-domain.com/scan/logs?lines=5000&grep=OPTION_CHAIN_FAIL

# Last 3000 lines containing get_option_chain or find_strike
GET https://your-domain.com/scan/logs?lines=3000&grep=get_option_chain
```

The agent can call this from Cursor; if your backend is public, it will work without opening SSH.

### Making sure the agent uses the right URL

- If you use a domain (e.g. www.tradewithcto.com), ensure the backend is served on that host and tell the agent to use that base URL for `/scan/logs`.
- If you only expose the app on the EC2 private IP, the agent still cannot reach it; use Option 2 or put the backend behind a public URL.

---

## Option 2: Allow Cursor IPs for SSH

If you want the agent to run commands on EC2 via SSH, you must allow **Cursor’s egress IP ranges** in your EC2 security group (inbound, port 22).

### 1. Get Cursor’s IP ranges

Cursor publishes current egress IPs (CIDRs) here:

- **Docs:** [Cloud Agent Egress IP Ranges](https://cursor.com/docs/cloud-agent/egress-ip-ranges)
- **JSON:** `https://cursor.com/docs/ips.json`

The JSON has a structure like:

```json
{"cloudAgents":{"us1":["18.246.172.173/32", ...], "us1p":[...], "us3":[...], ...}}
```

There are **many** CIDRs (200+). AWS security groups have a **limit of 60 inbound rules** per security group, so you **cannot** add every Cursor IP as a separate rule.

### 2. Practical approaches

- **Pick one region:** Choose one cluster (e.g. `us4p`) and add only those CIDRs. The agent might run in another region sometimes, so this can be unreliable.
- **Use a script:** Periodically fetch `https://cursor.com/docs/ips.json`, flatten the CIDRs, and add/update rules (e.g. via AWS CLI). You may need multiple security groups or to replace old rules with new ones to stay under the 60-rule limit.
- **Refresh regularly:** Cursor may change IPs; re-fetch the JSON and update rules when you need agent SSH access to work again.

### 3. Add rules (example for one CIDR)

```bash
# Replace SECURITY_GROUP_ID and REGION
aws ec2 authorize-security-group-ingress \
  --group-id SECURITY_GROUP_ID \
  --protocol tcp \
  --port 22 \
  --cidr 34.209.9.8/32 \
  --region REGION
```

Repeat for each CIDR you want to allow (up to 60 rules total for that group).

---

## Summary

| Method              | Pros                                      | Cons                                      |
|---------------------|-------------------------------------------|-------------------------------------------|
| **API + `?grep=`**  | No SSH, no new firewall rules, works from Cursor | Backend must be reachable from the internet |
| **Allow Cursor IPs**| Agent can SSH and run any command         | 60-rule limit; many IPs; need to maintain |

Recommendation: use **Option 1** (public backend + `GET /scan/logs?lines=...&grep=...`) so the Cursor agent can inspect scan logs directly. Use Option 2 only if you need the agent to run arbitrary commands on the server.

---

## EC2 public IP change and HTTPS (GitHub Actions)

- **GitHub Actions deploy:** Set repository secret `EC2_HOST` to the current public IP (e.g. `3.6.199.247`). In the repo: **Settings → Secrets and variables → Actions**, or with the GitHub CLI: `gh secret set EC2_HOST --body "3.6.199.247"` (after `gh auth login`).
- **SSL certificates:** Certs from Let's Encrypt (used in `scripts/nginx-tradentical.conf` under `/etc/letsencrypt/live/tradentical.com/`) are tied to **domain names**, not to the EC2 IP. You do **not** need a new certificate merely because the instance IP changed. Point DNS (A records) for your domains at the new IP, then on the new server either run `scripts/setup_ssl_tradentical.sh` / certbot again, or migrate `/etc/letsencrypt` from the old host with care. Browsing by **IP** over HTTPS may still show a certificate warning; use `https://your-domain.com` for a valid chain.
