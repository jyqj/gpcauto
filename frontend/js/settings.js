import { $, showToast } from './utils.js';
import * as api from './api.js';

export function initSettings() {
  $('#btnSaveSettings').addEventListener('click', saveSettings);
}

export async function loadSettings() {
  try {
    const { settings } = await api.settings.get();
    $('#settAdsApi').value = settings.ads_api || '';
    $('#settAdsKey').value = settings.ads_api_key || '';
    $('#settTabmailUrl').value = settings.tabmail_url || '';
    $('#settTabmailAdminKey').value = settings.tabmail_admin_key || '';
    $('#settTabmailTenantId').value = settings.tabmail_tenant_id || '';
    $('#settTabmailZoneId').value = settings.tabmail_zone_id || '';
  } catch (e) { showToast('加载设置失败', 'error'); }
}

async function saveSettings() {
  const body = {
    ads_api: $('#settAdsApi').value.trim(),
    ads_api_key: $('#settAdsKey').value.trim(),
    tabmail_url: $('#settTabmailUrl').value.trim(),
    tabmail_admin_key: $('#settTabmailAdminKey').value.trim(),
    tabmail_tenant_id: $('#settTabmailTenantId').value.trim(),
    tabmail_zone_id: $('#settTabmailZoneId').value.trim(),
  };
  await api.settings.update(body);
  showToast('设置已保存', 'success');
}
