ZK-ERP-SYNC — SETUP ON CLIENT SERVER
=====================================

FILES IN THIS FOLDER:
  zk_erp_sync.py              - the main script
  mock_punch_generator.py     - only for testing without real devices (optional on client server)
  requirements.txt            - Python packages needed
  config.server.example.ini   - template config, copy to config.ini and edit
  config.local.example.ini    - not needed on client server (used for laptop testing only)

INSTALL (run on the CLIENT SERVER terminal, via RustDesk):

  sudo mkdir -p /opt/zk-erp-sync
  sudo chown $USER:$USER /opt/zk-erp-sync
  cp zk_erp_sync.py mock_punch_generator.py requirements.txt config.server.example.ini /opt/zk-erp-sync/
  cd /opt/zk-erp-sync
  python3 -m venv venv
  venv/bin/pip install -r requirements.txt
  mkdir -p logs data
  cp config.server.example.ini config.ini
  chmod 600 config.ini
  nano config.ini      # fill in url, api_key, api_secret, device IPs

VERIFY:
  venv/bin/python zk_erp_sync.py --test-erp
  venv/bin/python zk_erp_sync.py --test-devices
  venv/bin/python zk_erp_sync.py --list-users
  venv/bin/python zk_erp_sync.py --dry-run

RUN FOR REAL (once verified):
  venv/bin/python zk_erp_sync.py -v
  tail -30 logs/sync.log

SCHEDULE WITH CRON (every 5 min, as the frappe user):
  sudo chown -R frappe:frappe /opt/zk-erp-sync
  sudo -u frappe crontab -e
  # add this single line, save, exit:
  */5 * * * * /opt/zk-erp-sync/venv/bin/python /opt/zk-erp-sync/zk_erp_sync.py --config /opt/zk-erp-sync/config.ini >> /opt/zk-erp-sync/logs/cron.log 2>&1

CONFIRM CRON IS FIRING:
  sudo -u frappe crontab -l
  journalctl -u cron -f
  tail -f /opt/zk-erp-sync/logs/sync.log
