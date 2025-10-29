// ===== Logs de ambiente (debug) =====
console.log('ENV CHECK', {
  SUPABASE_URL: !!process.env.SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY: !!process.env.SUPABASE_SERVICE_ROLE_KEY,
  FB_TOKEN: !!process.env.FB_TOKEN,
  FB_API_VERSION: process.env.FB_API_VERSION,
  ACCOUNT_IDS: process.env.ACCOUNT_IDS
});

import axios from 'axios';
import { createClient } from '@supabase/supabase-js';

const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  FB_TOKEN,
  FB_API_VERSION = 'v19.0',
  ACCOUNT_IDS
} = process.env;

// Valida envs (e sai sem “crash”)
function fail(msg) {
  console.error('❌', msg);
  process.exit(1);
}
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) fail('Faltam SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY');
if (!FB_TOKEN) fail('Falta FB_TOKEN');
if (!ACCOUNT_IDS) fail('Falta ACCOUNT_IDS');

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const accounts = ACCOUNT_IDS.split(',').map(s => s.trim()).filter(Boolean);

const today = () => new Date().toISOString().slice(0, 10);
const yesterday = () => new Date(Date.now() - 24*60*60*1000).toISOString().slice(0, 10);

async function fetchInsightsAccount(adAccountId, since, until) {
  const base = `https://graph.facebook.com/${FB_API_VERSION}/act_${adAccountId}/insights`;
  const params = {
    fields: 'account_id,account_name,spend,impressions,clicks,date_start,date_stop',
    time_range: JSON.stringify({ since, until }),
    level: 'account',
    time_increment: 1,
    access_token: FB_TOKEN,
    limit: 5000
  };

  const out = [];
  let url = base;
  let nextParams = { ...params };

  while (url) {
    const res = await axios.get(url, { params: nextParams }).catch(err => {
      const data = err.response?.data;
      const code = data?.error?.code;
      const msg  = data?.error?.message || err.message;
      throw new Error(`FB API erro (acc ${adAccountId}) code=${code} msg=${msg}`);
    });

    const body = res.data;
    if (body?.data?.length) out.push(...body.data);
    if (body?.paging?.next) {
      url = body.paging.next;
      nextParams = {}; // a próxima já vem com querystring completa
    } else {
      url = null;
    }
  }
  return out;
}

async function upsert(rows) {
  if (!rows.length) return { count: 0 };

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

  if (error) throw new Error(`Supabase upsert erro: ${error.message}`);
  return { count: mapped.length };
}

async function main() {
  const since = yesterday(); // pega ontem e hoje pra evitar dia vazio
  const until = today();
  console.log(`🚀 Sync de ${since} até ${until} | contas=${accounts.join(',')}`);

  for (const acc of accounts) {
    try {
      console.log(`🔄 Buscando conta ${acc}...`);
      const data = await fetchInsightsAccount(acc, since, until);
      console.log(`📦 FB retornou ${data.length} linhas para ${acc}`);
      const { count } = await upsert(data);
      console.log(`✅ Gravadas ${count} linhas no Supabase para ${acc}`);
    } catch (e) {
      console.error(`🔥 Falha na conta ${acc}:`, e.message);
    }
  }

  console.log('🏁 Finalizado sem erros fatais.');
  process.exit(0); // encerra como sucesso (Railway pode ainda mostrar "crashed", ignore)
}

main().catch(e => {
  console.error('💥 Erro inesperado:', e.message);
  process.exit(1);
});
