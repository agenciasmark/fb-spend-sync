// --- ENV CHECK (para debug no Railway) ---
console.log('ENV CHECK', {
  SUPABASE_URL: !!process.env.SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY: !!process.env.SUPABASE_SERVICE_ROLE_KEY,
  FB_TOKEN: !!process.env.FB_TOKEN,
  FB_API_VERSION: process.env.FB_API_VERSION,
  ACCOUNT_IDS: process.env.ACCOUNT_IDS
});

import axios from 'axios';
import { createClient } from '@supabase/supabase-js';

// Captura as variáveis do ambiente
const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  FB_TOKEN,
  FB_API_VERSION = 'v19.0',
  ACCOUNT_IDS
} = process.env;

// Mensagens de erro mais claras
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY)
  throw new Error('Faltam SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY');
if (!FB_TOKEN)
  throw new Error('Falta FB_TOKEN');
if (!ACCOUNT_IDS)
  throw new Error('Falta ACCOUNT_IDS');

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const accounts = ACCOUNT_IDS.split(',').map(s => s.trim()).filter(Boolean);

const today = () => new Date().toISOString().slice(0, 10);
} = process.env;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error('Faltam SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY no .env');
}
if (!FB_TOKEN) throw new Error('Falta FB_TOKEN no .env');
if (!ACCOUNT_IDS) throw new Error('Falta ACCOUNT_IDS no .env (ex: 123,456)');

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const accounts = ACCOUNT_IDS.split(',').map(s => s.trim()).filter(Boolean);

const today = () => new Date().toISOString().slice(0, 10);

async function fetchInsightsAccount(adAccountId, since, until) {
  const url = `https://graph.facebook.com/${FB_API_VERSION}/act_${adAccountId}/insights`;
  const params = {
    fields: 'account_id,account_name,spend,impressions,clicks,date_start,date_stop',
    time_range: JSON.stringify({ since, until }),
    level: 'account',
    time_increment: 1,
    access_token: FB_TOKEN,
    limit: 5000
  };

  const out = [];
  let next = url;
  let nextParams = { ...params };

  while (next) {
    const res = await axios.get(next, { params: nextParams });
    const body = res.data;
    if (body?.data?.length) out.push(...body.data);
    if (body?.paging?.next) {
      next = body.paging.next;
      nextParams = {};
    } else {
      next = null;
    }
  }
  return out;
}

async function upsert(rows) {
  if (!rows.length) return;
  const mapped = rows.map(r => ({
    ad_account_id: r.account_id,
    date: r.date_start,
    spend: Number(r.spend || 0),
    impressions: Number(r.impressions || 0),
    clicks: Number(r.clicks || 0),
    campaign_name: null,
    adset_name: null,
    ad_name: null
  }));

  const { error } = await supabase
    .from('facebook_spend')
    .upsert(mapped, { onConflict: 'ad_account_id,date' });

  if (error) throw error;
}

async function main() {
  const since = today();
  const until = today();

  for (const acc of accounts) {
    console.log('> Buscando conta', acc);
    const data = await fetchInsightsAccount(acc, since, until);
    console.log(`  - ${data.length} linhas`);
    await upsert(data);
    console.log('  ✓ Gravado no Supabase');
  }
  console.log('Concluído.');
}

main().catch(err => {
  console.error('Erro:', err.response?.data || err.message);
  process.exit(1);
});
