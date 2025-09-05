export interface Env {
  DB: D1Database;
  R2: R2Bucket;
}

const HOURS_INTENTION_TTL = 48; // 48h (2 dias)
const DAYS_R2_TTL = 30; // 30 dias
const R2_TEMP_PREFIX = 'temp/'; // prefixo usado nos uploads

// Configs de performance
const DB_BATCH_SIZE = 100; // quantas intentions processar por batch (também usado como page size)
const R2_DELETE_CONCURRENCY = 10; // quantos deletes R2 paralelos por batch
const R2_LIST_LIMIT = 1000; // limit para env.R2.list

// Retry configs
const R2_DELETE_MAX_ATTEMPTS = 3; // tentativas por key
const R2_DELETE_BASE_DELAY_MS = 200; // base para backoff exponencial

export default {
  async scheduled(event: any, env: Env, ctx: ExecutionContext): Promise<void> {
    console.log(`Scheduler fired (cron = ${event?.cron ?? 'unknown'}) - ${new Date().toISOString()}`);

    ctx.waitUntil(
      (async () => {
        try {
          await cleanupOldIntentions(env);
          await cleanupOldTempObjects(env);
          console.log('Scheduler finished successfully.');
        } catch (err) {
          console.error('Scheduler error:', err);
        }
      })()
    );
  },
};

async function cleanupOldIntentions(env: Env) {
  const cutoff = new Date(Date.now() - HOURS_INTENTION_TTL * 60 * 60 * 1000).toISOString();
  console.log(`cleanupOldIntentions: cutoff ISO = ${cutoff}`);

  let totalDeletedIntentions = 0;
  let totalQrDeleteSuccess = 0;
  let totalQrDeleteFail = 0;
  let page = 0;

  // Cursor pagination state
  let lastCreatedAt: string | null = null;
  let lastId: string | null = null;

  while (true) {
    page++;
    // Build paged SQL with cursor (deterministic, stable while deleting rows)
    let selectSql: string;
    let params: any[] = [];

    if (!lastCreatedAt) {
      // first page
      selectSql = `
        SELECT intention_id, qr_code, created_at
        FROM intentions
        WHERE status = ? AND created_at <= ?
        ORDER BY created_at ASC, intention_id ASC
        LIMIT ?
      `;
      params = ['pending', cutoff, DB_BATCH_SIZE];
    } else {
      // subsequent pages: only return rows AFTER the last cursor
      selectSql = `
        SELECT intention_id, qr_code, created_at
        FROM intentions
        WHERE status = ? AND created_at <= ?
          AND (
            created_at > ?
            OR (created_at = ? AND intention_id > ?)
          )
        ORDER BY created_at ASC, intention_id ASC
        LIMIT ?
      `;
      params = ['pending', cutoff, lastCreatedAt, lastCreatedAt, lastId, DB_BATCH_SIZE];
    }

    const selectRes = await env.DB.prepare(selectSql).bind(...params).all();
    const rows = (selectRes as any).results ?? (selectRes as any) ?? [];

    if (!rows || rows.length === 0) {
      console.log(`cleanupOldIntentions: página ${page} vazia — fim.`);
      break;
    }

    console.log(`cleanupOldIntentions: página ${page} — processando ${rows.length} rows`);

    // Prepare cursor for next page based on last row returned in this page
    const lastRow = rows[rows.length - 1];
    lastCreatedAt = lastRow.created_at;
    lastId = lastRow.intention_id ?? lastRow.id;

    // Map intentionId -> qrKey (ou null se não tiver)
    const intentToKey = new Map<string, string | null>();
    const keys: string[] = [];

    for (const r of rows) {
      const intentionId = (r.intention_id ?? r.id) as string | undefined;
      if (!intentionId) {
        console.warn('cleanupOldIntentions: encontrou linha sem intention_id, pulando', r);
        continue;
      }

      const qrCodeUrl = r.qr_code ?? r.QR_CODE;
      let key: string | null = null;
      if (qrCodeUrl && typeof qrCodeUrl === 'string') {
        key = extractR2KeyFromUrl(qrCodeUrl);
        if (key) keys.push(key);
        else console.warn(`cleanupOldIntentions: não foi possível extrair key do QR code URL: ${qrCodeUrl}`);
      }
      intentToKey.set(intentionId, key);
    }

    // Deletar as keys no R2 e obter resultados por key
    const deleteResults = await parallelDeleteR2KeysWithResult(env, keys, R2_DELETE_CONCURRENCY);

    // Decidir quais intentions podemos apagar:
    const intentionIdsToDelete: string[] = [];
    const intentionIdsToKeep: string[] = [];

    for (const [intentionId, key] of intentToKey.entries()) {
      if (!key) {
        // não tinha QR => deletar do DB
        intentionIdsToDelete.push(intentionId);
      } else {
        const ok = deleteResults.get(key);
        if (ok === true) {
          intentionIdsToDelete.push(intentionId);
          totalQrDeleteSuccess++;
        } else {
          intentionIdsToKeep.push(intentionId);
          totalQrDeleteFail++;
        }
      }
    }

    // Deletar do D1 apenas os que têm confirmação de remoção do R2 (ou sem key)
    if (intentionIdsToDelete.length > 0) {
      try {
        const placeholders = intentionIdsToDelete.map(() => '?').join(',');
        const deleteSql = `DELETE FROM intentions WHERE intention_id IN (${placeholders})`;
        await env.DB.prepare(deleteSql).bind(...intentionIdsToDelete).run();
        console.log(`cleanupOldIntentions: deleted ${intentionIdsToDelete.length} intentions (batch)`);
        totalDeletedIntentions += intentionIdsToDelete.length;
      } catch (err) {
        console.error('cleanupOldIntentions: erro ao deletar batch de intentions:', err);
        // fallback: deletar individualmente
        for (const id of intentionIdsToDelete) {
          try {
            await env.DB.prepare(`DELETE FROM intentions WHERE intention_id = ?`).bind(id).run();
            totalDeletedIntentions++;
            console.log(`cleanupOldIntentions: deleted intention ${id} (fallback)`);
          } catch (e) {
            console.error(`Erro ao deletar intention ${id} no fallback:`, e);
          }
        }
      }
    }

    // Para os que ficaram (QR não deletado), apenas logamos e deixamos para a próxima execução.
    if (intentionIdsToKeep.length > 0) {
      console.warn(
        `cleanupOldIntentions: página ${page} — ${intentionIdsToKeep.length} intentions mantidas para retry por falha ao deletar QR no R2. Exemplo IDs: ${intentionIdsToKeep
          .slice(0, 5)
          .join(', ')}${intentionIdsToKeep.length > 5 ? ', ...' : ''}`
      );
    }

    // continue para próxima página (cursor já atualizado)
  }

  console.log(
    `cleanupOldIntentions: resumo: intentions apagadas=${totalDeletedIntentions}, QR delete success=${totalQrDeleteSuccess}, QR delete fail=${totalQrDeleteFail}`
  );
}

/**
 * Deleta keys no R2 em batches com limite de concorrência.
 * Retorna Map<key, boolean> (true = deletado, false = falha)
 * Usa retry com backoff para erros transitórios e trata "not found" como sucesso.
 */
async function parallelDeleteR2KeysWithResult(env: Env, keys: string[], concurrency = 10): Promise<Map<string, boolean>> {
  const results = new Map<string, boolean>();
  if (!keys || keys.length === 0) return results;

  // dividir em batches de concorrência
  for (let i = 0; i < keys.length; i += concurrency) {
    const batch = keys.slice(i, i + concurrency);

    const promises = batch.map(async (key) => {
      const ok = await deleteR2WithRetry(env, key, R2_DELETE_MAX_ATTEMPTS);
      results.set(key, ok);
      return { key, ok };
    });

    // Espera o batch
    await Promise.all(promises);
  }

  return results;
}

/**
 * Tenta deletar uma key no R2 com retries e backoff exponencial + jitter.
 * Retorna true se deletado (ou já inexistente), false se falhou após tentativas.
 */
async function deleteR2WithRetry(env: Env, key: string, maxAttempts = 3): Promise<boolean> {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      await env.R2.delete(key);
      console.log(`deleteR2WithRetry: deleted ${key} (attempt ${attempt})`);
      return true;
    } catch (err: any) {
      // Tratar "not found" como sucesso — evita bloquear por arquivos já removidos
      const msg = String(err?.message ?? err).toLowerCase();
      const status = err?.status;
      if (status === 404 || /not.*found/i.test(msg)) {
        console.log(`deleteR2WithRetry: ${key} não existia (tratado como sucesso)`);
        return true;
      }

      console.error(`deleteR2WithRetry: erro ao deletar ${key} (attempt ${attempt}):`, err);

      if (attempt === maxAttempts) {
        console.error(`deleteR2WithRetry: falhou permanentemente ao deletar ${key} após ${maxAttempts} tentativas`);
        return false;
      }

      // backoff exponencial com jitter
      const baseDelay = R2_DELETE_BASE_DELAY_MS * Math.pow(2, attempt - 1);
      const jitter = Math.floor(Math.random() * 200);
      const delay = baseDelay + jitter;
      await new Promise((res) => setTimeout(res, delay));
    }
  }

  return false;
}

function extractR2KeyFromUrl(url: string): string | null {
  try {
    const u = new URL(url);
    // Suporta caminhos como '/file/<key>' ou '/<prefix>/<key>'
    const parts = u.pathname.split('/file/');
    if (parts.length === 2 && parts[1]) {
      return parts[1];
    }
    // Caso a key esteja simplesmente no pathname (ex: /temp/abc.jpg)
    const pathname = u.pathname.startsWith('/') ? u.pathname.slice(1) : u.pathname;
    if (pathname) return pathname;
    return null;
  } catch {
    return null;
  }
}

async function cleanupOldTempObjects(env: Env) {
  const daysMs = DAYS_R2_TTL * 24 * 60 * 60 * 1000;
  const now = Date.now();
  console.log(`cleanupOldTempObjects: removing objects older than ${DAYS_R2_TTL} days`);

  let cursor: string | undefined = undefined;
  let totalDeleted = 0;
  let totalChecked = 0;
  let totalDeleteFails = 0;

  do {
    const listOpts: any = { prefix: R2_TEMP_PREFIX, limit: R2_LIST_LIMIT };
    if (cursor) listOpts.cursor = cursor;

    let listResult;
    try {
      listResult = await env.R2.list(listOpts);
    } catch (err) {
      console.error('cleanupOldTempObjects: erro ao listar R2:', err);
      break;
    }

    const items = ((listResult.objects ?? listResult) as any[]) ?? [];

    if (!items || items.length === 0) {
      if ((listResult as any).truncated) {
        cursor = (listResult as any).cursor;
        continue;
      } else {
        break;
      }
    }

    const keysToDelete: string[] = [];
    for (const obj of items) {
      totalChecked++;
      let uploadedDate: Date | null = null;
      try {
        if (obj.uploaded instanceof Date) uploadedDate = obj.uploaded;
        else if (typeof obj.uploaded === 'string') uploadedDate = new Date(obj.uploaded);
        else if (obj.customMetadata && obj.customMetadata.createdAt) uploadedDate = new Date(obj.customMetadata.createdAt);
        else if (obj.uploadedAt) uploadedDate = new Date(obj.uploadedAt);
        else if (obj.created_at) uploadedDate = new Date(obj.created_at);
      } catch {
        uploadedDate = null;
      }

      if (!uploadedDate || Number.isNaN(uploadedDate.getTime())) {
        console.log(`cleanupOldTempObjects: não foi possível determinar data para ${obj.key} — pulando`);
        continue;
      }

      const ageMs = now - uploadedDate.getTime();
      if (ageMs > daysMs) keysToDelete.push(obj.key);
    }

    // deletar chaves em paralelo controlado (usando função que retorna resultados)
    const deleteResults = await parallelDeleteR2KeysWithResult(env, keysToDelete, R2_DELETE_CONCURRENCY);
    for (const k of keysToDelete) {
      if (deleteResults.get(k) === true) totalDeleted++;
      else totalDeleteFails++;
    }

    cursor = (listResult as any).truncated ? (listResult as any).cursor : undefined;
    if (!cursor) break;
  } while (true);

  console.log(
    `cleanupOldTempObjects: checked ${totalChecked} objects, deleted ${totalDeleted}, failed deletes ${totalDeleteFails}`
  );
}

// util: chunk array (caso você prefira)
function chunkArray<T>(arr: T[], size: number): T[][] {
  const res: T[][] = [];
  for (let i = 0; i < arr.length; i += size) res.push(arr.slice(i, i + size));
  return res;
}
