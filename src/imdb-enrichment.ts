import { supabase } from './supabase';
import * as countries from 'i18n-iso-countries';
import * as dotenv from 'dotenv';
dotenv.config();

// @ts-ignore
countries.registerLocale(require('i18n-iso-countries/langs/en.json'));

const RAPIDAPI_KEY  = process.env.RAPIDAPI_KEY!;
const RAPIDAPI_HOST = 'imdb236.p.rapidapi.com';
const LIMIT         = parseInt(process.env.LIMIT       || '100');
const STALE_DAYS    = parseInt(process.env.STALE_DAYS  || '30');
const CONCURRENCY   = parseInt(process.env.CONCURRENCY || '4');
const SLEEP_MS      = parseInt(process.env.SLEEP_MS    || '2500');
const WORKFLOW_ID   = parseInt(process.env.WORKFLOW_ID || '0');

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

// ─── COUNTRY CODE HELPER ─────────────────────────────────────────────────────

function getIsoCode(location: string | null): string | null {
    if (!location) return null;
    const parts = location.split(',').map(p => p.trim());
    const countryName = parts[parts.length - 1];
    // @ts-ignore
    let code = countries.getAlpha2Code(countryName, 'en');
    if (!code) {
        const overrides: Record<string, string> = {
            'USA': 'US', 'UK': 'GB', 'U.K.': 'GB', 'U.S.A.': 'US',
            'Scotland': 'GB', 'England': 'GB', 'Wales': 'GB',
            'Soviet Union': 'RU', 'Yugoslavia': 'RS', 'West Germany': 'DE',
        };
        code = overrides[countryName] || null;
    }
    return code || null;
}

// ─── IMDB API ────────────────────────────────────────────────────────────────

async function fetchImdbData(nmId: string, retries = 2): Promise<any | null> {
    try {
        const res = await fetch(`https://${RAPIDAPI_HOST}/api/imdb/name/${nmId}`, {
            headers: {
                'X-Rapidapi-Key':  RAPIDAPI_KEY,
                'X-Rapidapi-Host': RAPIDAPI_HOST,
                'accept': 'application/json',
            },
        });
        if (res.status === 429 && retries > 0) {
            console.log(`   ⏳ 429 Rate Limit for ${nmId}. Retrying in 10s...`);
            await sleep(10000);
            return fetchImdbData(nmId, retries - 1);
        }
        if (res.status === 404) return null;
        if (!res.ok) throw new Error(`HTTP ${res.status}: ${res.statusText}`);
        return await res.json();
    } catch (err: any) {
        console.error(`   ❌ RapidAPI error for ${nmId}: ${err.message}`);
        return null;
    }
}

// ─── SUPABASE WORKFLOW SUMMARY ───────────────────────────────────────────────

async function updateWorkflowSummary(
    status: 'success' | 'failure',
    summary: Record<string, unknown>,
    durationSecs: number
) {
    if (!WORKFLOW_ID) return;
    const { error } = await supabase.rpc('log_workflow_run', {
        p_workflow_id:   WORKFLOW_ID,
        p_status:        status,
        p_duration_secs: durationSecs,
        p_summary:       summary,
    });
    if (error) console.warn(`   ⚠️  Workflow summary update failed: ${error.message}`);
    else console.log(`   📊 Workflow summary logged to Supabase`);
}

// ─── MAIN ────────────────────────────────────────────────────────────────────

async function run() {
    const runStart = Date.now();
    console.log(`🎬 IMDb Enrichment — up to ${LIMIT} records (stale > ${STALE_DAYS}d, concurrency: ${CONCURRENCY})\n`);

    const staleThreshold = new Date(Date.now() - STALE_DAYS * 24 * 60 * 60 * 1000).toISOString();

    // Fetch stale IMDB social rows — oldest check_imdb_enrichment first
    const { data: socials, error } = await supabase
        .from('hb_socials')
        .select('id, identifier, name, linked_talent, check_imdb_enrichment')
        .eq('type', 'IMDB')
        .or(`check_imdb_enrichment.is.null,check_imdb_enrichment.lt.${staleThreshold}`)
        .order('check_imdb_enrichment', { ascending: true, nullsFirst: true })
        .limit(LIMIT);

    if (error) throw error;
    if (!socials?.length) {
        console.log('✅ No stale IMDb profiles to enrich.');
        const durationSecs = Math.round((Date.now() - runStart) / 1000);
        await updateWorkflowSummary('success', { enriched: 0, run_at: new Date().toISOString() }, durationSecs);
        return;
    }

    console.log(`   Found ${socials.length} stale IMDb profiles.\n`);

    // Pre-load linked talent records in one batch query
    const talentIds = [...new Set(socials.map(s => s.linked_talent).filter(Boolean))];
    const { data: talents } = await supabase
        .from('hb_talent')
        .select('id, name, image, biography, birth_location, birth_country, date_birthdate, date_deathdate, stats_height')
        .in('id', talentIds);
    const talentMap = new Map(talents?.map(t => [t.id, t]) || []);
    console.log(`   ✅ Pre-loaded ${talentMap.size} linked talent records\n`);

    // Pre-load valid country codes
    const { data: validCountryData } = await supabase.from('countries').select('country_code');
    const validCodes = new Set(validCountryData?.map(c => c.country_code) || []);

    const socialUpdates: any[] = [];
    const talentUpdates: any[] = [];
    let enrichedCount = 0, failedCount = 0;

    // Process in parallel batches
    for (let i = 0; i < socials.length; i += CONCURRENCY) {
        const chunk = socials.slice(i, i + CONCURRENCY);

        await Promise.all(chunk.map(async (social) => {
            console.log(`🔍 ${social.name || social.identifier} (${social.identifier})`);
            const now = new Date().toISOString();

            const imdbData = await fetchImdbData(social.identifier);

            if (!imdbData) {
                // Stamp so we don't retry every run
                socialUpdates.push({ id: social.id, check_imdb_enrichment: now });
                failedCount++;
                return;
            }

            socialUpdates.push({
                id:                    social.id,
                name:                  imdbData.name          || social.name,
                description:           imdbData.biography     || null,
                image:                 imdbData.primaryImage  || null,
                top_media:             imdbData.knownForTitles || null,
                detailed_array:        imdbData,
                check_imdb_enrichment: now,
                updated_at:            now,
            });
            enrichedCount++;
            console.log(`   ✅ Enriched: ${imdbData.name || social.identifier}`);

            // Hydrate hb_talent — only fill blank fields
            if (social.linked_talent && talentMap.has(social.linked_talent)) {
                const t = talentMap.get(social.linked_talent)!;
                const tUpdate: any = { id: t.id };
                let changed = false;

                if (!t.image          && imdbData.primaryImage)  { tUpdate.image          = imdbData.primaryImage;  changed = true; }
                if (!t.biography      && imdbData.biography)     { tUpdate.biography      = imdbData.biography;     changed = true; }
                if (!t.birth_location && imdbData.birthLocation) { tUpdate.birth_location = imdbData.birthLocation; changed = true; }
                if (!t.date_birthdate && imdbData.birthDate)     { tUpdate.date_birthdate = imdbData.birthDate;     changed = true; }
                if (!t.date_deathdate && imdbData.deathDate)     { tUpdate.date_deathdate = imdbData.deathDate;     changed = true; }
                if (!t.stats_height   && imdbData.height)        { tUpdate.stats_height   = imdbData.height;        changed = true; }

                if (!t.birth_country && imdbData.birthLocation) {
                    const iso = getIsoCode(imdbData.birthLocation);
                    if (iso && validCodes.has(iso.toUpperCase())) {
                        tUpdate.birth_country = iso.toUpperCase();
                        changed = true;
                    }
                }

                if (changed) {
                    talentUpdates.push(tUpdate);
                    console.log(`   ✨ Queued talent hydration: ${t.id}`);
                }
            }
        }));

        await sleep(SLEEP_MS);
    }

    // ── Batch apply all updates ──────────────────────────────────────────────
    console.log(`\n💾 Applying batch updates...`);

    if (socialUpdates.length > 0) {
        const { error: sErr } = await supabase.from('hb_socials').upsert(socialUpdates);
        if (sErr) console.error(`❌ Social batch error: ${sErr.message}`);
        else console.log(`   ✅ Updated ${socialUpdates.length} social records`);
    }

    if (talentUpdates.length > 0) {
        const { error: tErr } = await supabase.from('hb_talent').upsert(talentUpdates);
        if (tErr) console.error(`❌ Talent batch error: ${tErr.message}`);
        else console.log(`   ✅ Hydrated ${talentUpdates.length} talent profiles`);
    }

    const durationSecs = Math.round((Date.now() - runStart) / 1000);
    const summaryObj = {
        profiles_processed: socials.length,
        enriched:           enrichedCount,
        failed:             failedCount,
        talent_hydrated:    talentUpdates.length,
        run_at:             new Date().toISOString(),
    };
    console.log(`\n🎉 Done! ${enrichedCount} enriched, ${failedCount} failed, ${talentUpdates.length} talent hydrated (${durationSecs}s)`);
    await updateWorkflowSummary('success', summaryObj, durationSecs);
}

run().catch(async (err) => {
    console.error('🔥 Fatal:', err.message);
    await updateWorkflowSummary('failure', { error: err.message, run_at: new Date().toISOString() }, 0);
    process.exit(1);
});
