export interface Env {
  DB: D1Database;
  ASSETS: R2Bucket;
}

const HOURS_INTENTION_TTL = 2; // 2 horas
const DAYS_R2_TTL = 30;        // 30 dias
const R2_TEMP_PREFIX = "temp/"; // prefixo usado nos uploads

export default {
  async scheduled(event: any, env: Env, ctx: ExecutionContext): Promise<void> {
    console.log(`Scheduler fired (cron = ${event?.cron ?? "unknown"}) - ${new Date().toISOString()}`);

    try {
      await cleanupOldIntentions(env);
      await cleanupOldTempObjects(env);

      console.log("Scheduler finished successfully.");
    } catch (err) {
      console.error("Scheduler error:", err);
    }
  },
};

async function cleanupOldIntentions(env: Env) {
  const cutoff = new Date(Date.now() - HOURS_INTENTION_TTL * 60 * 60 * 1000).toISOString();
  console.log(`cleanupOldIntentions: cutoff ISO = ${cutoff}`);

  const selectSql = `
    SELECT intention_id
    FROM intentions
    WHERE status != ? AND created_at <= ?
  `;
  const selectStmt = env.DB.prepare(selectSql).bind("approved", cutoff);
  const selectRes = await selectStmt.all();
  const rows = (selectRes && (selectRes as any).results) || (selectRes as any) || [];

  if (!rows || rows.length === 0) {
    console.log("cleanupOldIntentions: nenhuma intention antiga encontrada.");
    return;
  }

  console.log(`cleanupOldIntentions: encontradas ${rows.length} intentions para apagar.`);

  let deleted = 0;
  for (const r of rows) {
    const intentionId = r.intention_id ?? r.id ?? r.INTENTION_ID;
    if (!intentionId) continue;

    try {
      await env.DB.prepare(`DELETE FROM intentions WHERE intention_id = ?`).bind(intentionId).run();
      deleted++;
      console.log(`cleanupOldIntentions: deleted intention ${intentionId}`);
    } catch (err) {
      console.error(`cleanupOldIntentions: erro ao deletar intention ${intentionId}:`, err);
    }
  }

  console.log(`cleanupOldIntentions: total deletado = ${deleted}`);
}

async function cleanupOldTempObjects(env: Env) {
  const daysMs = DAYS_R2_TTL * 24 * 60 * 60 * 1000;
  const now = Date.now();
  console.log(`cleanupOldTempObjects: removing objects older than ${DAYS_R2_TTL} days`);

  let cursor: string | undefined = undefined;
  let totalDeleted = 0;
  let totalChecked = 0;

  do {
    const listOpts: any = { prefix: R2_TEMP_PREFIX, limit: 1000 };
    if (cursor) listOpts.cursor = cursor;

    const listResult = await env.ASSETS.list(listOpts);
    const items = (listResult.objects ?? listResult) as any[];

    if (!items || items.length === 0) {
      if (listResult.truncated) {
        cursor = (listResult as any).cursor;
        continue;
      } else {
        break;
      }
    }

    for (const obj of items) {
      totalChecked++;
      const uploadedRaw = obj.uploaded ?? obj.time ?? obj.lastModified ?? obj["last-modified"];
      let uploadedDate: Date | null = null;

      if (typeof uploadedRaw === "string") {
        uploadedDate = new Date(uploadedRaw);
      } else if (typeof uploadedRaw === "number") {
        uploadedDate = new Date(uploadedRaw);
      } else if (obj["httpMetadata"] && obj["httpMetadata"]["date"]) {
        uploadedDate = new Date(obj["httpMetadata"]["date"]);
      }

      if (!uploadedDate || isNaN(uploadedDate.getTime())) {
        console.log(`cleanupOldTempObjects: não foi possível determinar data para ${obj.key} — pulando`);
        continue;
      }

      const ageMs = now - uploadedDate.getTime();
      if (ageMs > daysMs) {
        try {
          await env.ASSETS.delete(obj.key);
          totalDeleted++;
          console.log(`cleanupOldTempObjects: deleted ${obj.key} (age ${(ageMs / (24 * 3600 * 1000)).toFixed(1)} days)`);
        } catch (err) {
          console.error(`cleanupOldTempObjects: erro ao deletar ${obj.key}:`, err);
        }
      }
    }

    cursor = (listResult as any).cursor;
    if (!cursor) break;
  } while (true);

  console.log(`cleanupOldTempObjects: checked ${totalChecked} objects, deleted ${totalDeleted}`);
}
