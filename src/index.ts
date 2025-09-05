export interface Env {
	DB: D1Database;
	R2: R2Bucket;
}

const HOURS_INTENTION_TTL = 48; // 2 horas
const DAYS_R2_TTL = 30; // 30 dias
const R2_TEMP_PREFIX = 'temp/'; // prefixo usado nos uploads

export default {
	async scheduled(event: any, env: Env, ctx: ExecutionContext): Promise<void> {
		console.log(`Scheduler fired (cron = ${event?.cron ?? 'unknown'}) - ${new Date().toISOString()}`);

		try {
			await cleanupOldIntentions(env);
			await cleanupOldTempObjects(env);

			console.log('Scheduler finished successfully.');
		} catch (err) {
			console.error('Scheduler error:', err);
		}
	},
};

async function cleanupOldIntentions(env: Env) {
	const cutoff = new Date(Date.now() - HOURS_INTENTION_TTL * 60 * 60 * 1000).toISOString();
	console.log(`cleanupOldIntentions: cutoff ISO = ${cutoff}`);

	const selectSql = `
    SELECT intention_id, qr_code
    FROM intentions
    WHERE status = ? AND created_at <= ?
  `;
	const selectRes = await env.DB.prepare(selectSql).bind('pending', cutoff).all();
	const rows = (selectRes as any).results ?? (selectRes as any) ?? [];

	if (!rows.length) {
		console.log('cleanupOldIntentions: nenhuma intention antiga encontrada.');
		return;
	}

	console.log(`cleanupOldIntentions: encontradas ${rows.length} intentions para apagar.`);

	let deletedCount = 0;

	for (const r of rows) {
		const intentionId = r.intention_id ?? r.id;
		const qrCodeUrl = r.qr_code ?? r.QR_CODE;

		if (!intentionId) continue;

		try {
			await env.DB.prepare(`DELETE FROM intentions WHERE intention_id = ?`).bind(intentionId).run();
			console.log(`cleanupOldIntentions: deleted intention ${intentionId}`);
		} catch (err) {
			console.error(`Erro ao deletar intention ${intentionId}:`, err);
			continue;
		}

		// Se existir QR code, processar remoção do arquivo
		if (qrCodeUrl && typeof qrCodeUrl === 'string') {
			const key = extractR2KeyFromUrl(qrCodeUrl);
			if (key) {
				try {
					await env.R2.delete(key);
					console.log(`cleanupOldIntentions: deleted QR code object: ${key}`);
				} catch (err) {
					console.error(`Erro ao deletar QR code ${key}:`, err);
				}
			} else {
				console.warn(`Não foi possível extrair key do QR code URL: ${qrCodeUrl}`);
			}
		}

		deletedCount++;
	}

	console.log(`cleanupOldIntentions: total intentions apagadas = ${deletedCount}`);
}

// Função utilitária para extrair a "key" do R2 a partir de uma URL completa
function extractR2KeyFromUrl(url: string): string | null {
	try {
		const u = new URL(url);
		const parts = u.pathname.split('/file/');
		if (parts.length === 2) {
			return parts[1];
		}
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

	do {
		const listOpts: any = { prefix: R2_TEMP_PREFIX, limit: 1000 };
		if (cursor) listOpts.cursor = cursor;

		const listResult = await env.R2.list(listOpts);
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
			const uploadedRaw = obj.uploaded ?? obj.time ?? obj.lastModified ?? obj['last-modified'];
			//let uploadedDate: Date | null = null;
			const uploadedDate: Date | null = obj.uploaded instanceof Date ? obj.uploaded : null;

			if (!uploadedDate) {
				console.log(`cleanupOldTempObjects: não foi possível determinar data para ${obj.key} — pulando`);
				continue;
			}

			const ageMs = now - uploadedDate.getTime();
			if (ageMs > daysMs) {
				try {
					await env.R2.delete(obj.key);
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
