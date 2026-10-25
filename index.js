/**
 * nicoAutoReroll —— 酒馆自动重Roll
 * ------------------------------------------------------------
 * 作用：当生成请求失败、酒馆没有输出内容就停止时，自动帮用户按下「重新生成」。
 *
 * 覆盖的失败场景：
 *   - HTTP 429（限流，可读取 Retry-After 自动延长等待）
 *   - HTTP 5xx（500 / 502 / 503 / 504 等服务器错误）
 *   - HTTP 403（拒绝访问，可开关）
 *   - 其他 HTTP 4xx（400 / 401 / 404 等，默认重试，可关）
 *   - 网络错误 / 连接中断（Failed to fetch 等）
 *   - 请求成功但回复为空（空回复、流式中途断开无输出、生成跑完却没有任何 AI 消息）
 *   - 模型拒答（道歉 / 拒绝话术、跳出角色的“作为 AI”，内置中英句式库 + 可自定义）
 *   - 流式中断但已有部分内容（默认关，开启后会删掉残文重新生成）
 *
 * 设计要点：
 *   1. 包装 window.fetch，精确读取生成接口的 HTTP 状态码与 Retry-After（toast 之外的硬证据）；
 *   2. 包装 toastr.error，捕获生成窗口内的错误提示（含 SSE 流中错误）；
 *   3. 监听 GENERATION_STARTED / ENDED / STOPPED / CHAT_CHANGED 事件做状态机判定；
 *   4. 用户主动点「停止」→ 绝不重试；等待期间用户手动发消息 / 切换聊天 → 自动放弃；
 *   5. 首次失败约 1 秒即重试，之后指数退避（1 → 2 → 4 → 8 → 16 秒，上限 30 秒）+ 抖动；
 *   6. 防误伤三道闸：HTTP 状态码硬证据优先、空回复 350ms 二次确认、真正触发前再复核一次；
 *   7. 重试动作与用户手动点「重新生成」完全一致（#option_regenerate），swipe 走官方 auto_swipe，
 *      单聊、群聊、空消息残留、未产生消息等情况都由酒馆原生逻辑处理；
 *   8. 不修改 SillyTavern 核心代码；禁用或删除本扩展即可完全移除。
 *
 * 作者：nico
 */

import { eventSource, event_types } from '../../../../scripts/events.js';

const MODULE_NAME = 'nicoAutoReroll';
const MODULE_VERSION = '1.2.2';
const SETTINGS_VERSION = 2;
const LOG_PREFIX = '[nicoAutoReroll]';

// ============================ 默认设置 ============================

const DEFAULT_SETTINGS = {
    enabled: true,              // 总开关
    maxRetries: 5,              // 最大重试次数（0 = 不重试）
    baseDelay: 1,               // 首次重试前等待秒数（约 1 秒，快速兜底）
    factor: 2,                  // 退避倍率：每次失败后等待时间 × 该值
    maxDelay: 30,               // 单次等待上限（秒）
    jitter: true,               // ±20% 随机抖动，避免多客户端同时重试
    respectRetryAfter: true,    // 429 时优先遵守服务器 Retry-After
    retryOn429: true,           // 429 限流
    retryOn5xx: true,           // 500 / 502 / 503 / 504 等
    retryOn403: true,           // 403 拒绝访问
    retryOn4xxOther: true,      // 其他 4xx（400 / 401 / 404 等）
    retryOnNetwork: true,       // 网络错误 / 连接中断
    retryOnEmpty: true,         // 空回复（200 成功但无正文）
    reasonOnlyAsEmpty: true,    // 只有推理（reasoning）没有正文也算空回复
    retryOnRefusal: true,       // 模型拒答（道歉 / 拒绝话术、"作为 AI"）
    refusalMaxLength: 300,      // 拒答软模式仅对短于该长度（字符）的回复生效，0 = 不限
    refusalCustomPatterns: '',  // 自定义拒答句式，每行一个（/正则/标志 或纯文本）
    retryOnPartial: false,      // 流式中断且已有部分内容（会删残文重生）
    retryOnGroup: true,         // 群聊中也启用
    showToast: true,            // 弹出通知
};

const SETTINGS_TYPES = Object.fromEntries(
    Object.entries(DEFAULT_SETTINGS).map(([k, v]) => [k, typeof v]),
);

// ============================ 生成接口白名单 ============================

const GENERATE_URL_PATTERNS = [
    /\/api\/backends\/chat-completions\/generate/,   // Chat Completion（OpenAI / Claude / 第三方中转等）
    /\/api\/backends\/text-completions\/generate/,   // Text Completion（Ooba / Tabby / vLLM 等）
    /\/api\/backends\/kobold\/generate/,             // KoboldAI 原生
    /\/api\/novelai\/generate(?:$|\?)/,              // NovelAI（排除 generate-image / generate-voice）
    /\/api\/horde\/generate-text/,                   // AI Horde
];

function isGenerateUrl(url) {
    try {
        return GENERATE_URL_PATTERNS.some((re) => re.test(String(url)));
    } catch {
        return false;
    }
}

// ============================ 状态 ============================

const watch = {
    active: false,          // 当前是否处于受监控的生成窗口
    token: 0,               // 每次生成开始时 +1，用于作废过期的延迟判定
    evaluatedToken: -1,     // 已完成判定的 token，防止 ENDED 重复判定
    confirmToken: -1,       // 已安排二次确认的 token，防止空回复复核重复排队
    type: null,             // 本次生成的类型
    chatId: null,           // 生成开始时的聊天 ID
    startLen: 0,            // 生成开始时的消息条数
    startAnchor: null,      // 生成开始时最后一条消息的对象引用（用于识别“零输出”）
    signals: [],            // 本次生成收集到的失败信号
    fetchCount: 0,          // 本次生成窗口内实际发出的生成接口请求数
    userStopped: false,     // 用户是否主动停止
    attempt: 0,             // 当前失败链已重试的次数
};

let ownRetry = false;              // 下一次 STARTED 是由本扩展触发的
let retryChainUntil = 0;           // 重试链宽限时间戳（覆盖群聊 wrapper / 工具递归的多次 STARTED）
let retryTimer = null;
let countdownTimer = null;

// ============================ 工具函数 ============================

function getCtx() {
    return window.SillyTavern.getContext();
}

function getSettings() {
    const ctx = getCtx();
    if (!ctx.extensionSettings[MODULE_NAME]) {
        ctx.extensionSettings[MODULE_NAME] = {};
    }
    const s = ctx.extensionSettings[MODULE_NAME];
    for (const [key, defaultValue] of Object.entries(DEFAULT_SETTINGS)) {
        if (typeof s[key] !== SETTINGS_TYPES[key]) {
            s[key] = defaultValue;
        }
    }
    return s;
}

function saveSettings() {
    try {
        getCtx().saveSettingsDebounced();
    } catch (err) {
        console.warn(LOG_PREFIX, '保存设置失败', err);
    }
}

/**
 * 旧版本设置升级：仅在版本号落后时执行，避免老用户缓存里的旧默认值覆盖新策略。
 * 返回是否发生了迁移。
 */
function migrateSettings(s) {
    if (s._version === SETTINGS_VERSION) return false;
    if (!s._version || s._version < 2) {
        // v1.1：首次重试提速到约 1 秒、等待上限 30 秒、所有 4xx 报错默认重试
        s.baseDelay = DEFAULT_SETTINGS.baseDelay;
        s.maxDelay = DEFAULT_SETTINGS.maxDelay;
        s.retryOn4xxOther = DEFAULT_SETTINGS.retryOn4xxOther;
    }
    s._version = SETTINGS_VERSION;
    return true;
}

function currentChatId() {
    try {
        return getCtx().getCurrentChatId() ?? '';
    } catch {
        return '';
    }
}

function isGenerating() {
    return document.body?.dataset?.generating === 'true';
}

function pushSignal(signal) {
    if (!watch.active) return;
    watch.signals.push({ ...signal, at: Date.now() });
    console.warn(LOG_PREFIX, '检测到失败信号：', signal);
}

// ============================ Hook：window.fetch ============================

function installFetchHook() {
    if (window.__nicoAutoRerollFetchHooked) return;
    window.__nicoAutoRerollFetchHooked = true;

    const originalFetch = window.fetch.bind(window);
    window.fetch = async function nicoAutoRerollFetch(...args) {
        let url = '';
        try {
            const input = args[0];
            url = typeof input === 'string' ? input : (input?.url ?? '');
        } catch { /* ignore */ }

        const watched = watch.active && isGenerateUrl(url);
        if (watched) watch.fetchCount += 1;

        try {
            const response = await originalFetch(...args);
            if (watched && response && !response.ok) {
                let retryAfter = null;
                try {
                    retryAfter = response.headers.get('retry-after');
                } catch { /* ignore */ }
                pushSignal({
                    kind: 'http',
                    status: response.status,
                    retryAfter,
                    url,
                });
            }
            return response;
        } catch (err) {
            if (watched) {
                pushSignal({
                    kind: 'network',
                    name: err?.name ?? '',
                    message: String(err?.message ?? err),
                    url,
                });
            }
            throw err;
        }
    };
}

// ============================ Hook：toastr.error ============================

function toastValueToText(value) {
    if (value == null) return '';
    if (typeof value === 'string') return value;
    if (typeof value === 'object' && value.message) return String(value.message);
    try {
        return JSON.stringify(value);
    } catch {
        return String(value);
    }
}

function installToastHook() {
    const tryInstall = () => {
        const toastr = window.toastr;
        if (!toastr || typeof toastr.error !== 'function') return false;
        if (toastr.error.__nicoAutoReroll) return true;

        const originalError = toastr.error;
        function wrappedError(message, title, ...rest) {
            try {
                if (watch.active) {
                    const text = `${toastValueToText(title)} ${toastValueToText(message)}`.trim().slice(0, 500);
                    pushSignal({ kind: 'toast', text });
                }
            } catch { /* ignore */ }
            return originalError.apply(this, [message, title, ...rest]);
        }
        wrappedError.__nicoAutoReroll = true;
        toastr.error = wrappedError;
        return true;
    };

    if (!tryInstall()) {
        let tries = 0;
        const timer = setInterval(() => {
            if (tryInstall() || ++tries > 40) clearInterval(timer);
        }, 500);
    }
}

// ============================ 失败判定 ============================

/**
 * 把酒馆消息正文（含 HTML）归一化为纯文本：去标签、解码常用实体、折叠空白。
 */
function normalizePlainText(raw) {
    return String(raw ?? '')
        .replace(/<br\s*\/?>/gi, ' ')
        .replace(/<[^>]+>/g, '')
        .replace(/&nbsp;/g, ' ')
        .replace(/&amp;/g, '&')
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&quot;/g, '"')
        .replace(/&#39;/g, "'")
        .replace(/\s+/g, ' ')
        .trim();
}

/**
 * 判断一条 AI 消息是否为空回复（无正文、无图片、无工具调用）
 */
function messageLooksEmpty(message, settings) {
    if (!message) return true;
    const extra = message.extra || {};

    // 有图片 / 工具调用产物 / 附件，不算空
    if (Array.isArray(extra.media) && extra.media.length > 0) return false;
    if (Array.isArray(extra.tool_invocations) && extra.tool_invocations.length > 0) return false;
    if (Array.isArray(extra.attachments) && extra.attachments.length > 0) return false;

    const raw = String(message.mes ?? '');

    // 正文里直接内嵌了图片 / 视频 / 音频 / iframe（部分中转把媒体直接写进 mes）
    if (/<(?:img|video|audio|iframe|object|embed)\b/i.test(raw)) return false;

    const text = normalizePlainText(raw);

    if (text && text !== '...' && text !== '…') return false;

    // 只有推理没有正文
    if (extra.reasoning && !settings.reasonOnlyAsEmpty) return false;

    return true;
}

// ----------------- 拒答（refusal）检测 -----------------

/**
 * 强模式：模型跳出角色时的自述话术。这类短语几乎只会出现在拒答中，
 * 不限长度、不看引号，命中即判拒答。
 */
const REFUSAL_STRONG_PATTERNS = [
    // 负向先行豁免 "AI researcher / engineer / character" 等角色设定文本
    /\bas an ai(?!\s+(?:researcher|engineer|developer|expert|scientist|student|hobbyist|writer|artist|character|companion))\b/i,
    /\bas a (?:large |generative |responsible )?language model\b/i,
    /\bi(?:'m| am) an? ai(?!\s+(?:researcher|engineer|developer|expert|scientist|student|writer|hobbyist|artist))\b/i,
    /\bagainst my (?:programming|guidelines|policies?|principles?|values)\b/i,
    /\bmy (?:programming|guidelines|policies?|training data) (?:prevents|doesn't allow|does not allow|won't let me|prohibits)\b/i,
    /\bi am an? (?:ai(?!\s+(?:researcher|engineer|developer|expert))|artificial intelligence|language model)\b/i,
    /作为(?:一个|一名)?\s*(?:AI|ai)(?:语言模型|大模型|助手)?(?:[，,。\s]|我)/,
    /作为(?:一个|一名)?\s*(?:人工智能|语言模型|大语言模型|智能助手)/,
    /我是(?:一个|一名)?(?:AI|ai)(?:语言模型|大模型|助手)?(?:[，,。\s]|我)/,
    /我是(?:一个|一名)?(?:人工智能|语言模型|大语言模型|智能助手)/,
    // 固定模板：「我无法继续执行涉及这些个人属性和指令的角色扮演、数据格式化或伪代码指令」
    /我(?:无法|不能)继续执行涉及[^。；\n]{0,80}(?:个人属性|角色扮演|数据格式化|伪代码)/,
    /涉及(?:这些)?(?:个人属性|敏感内容)[^。；\n]{0,40}(?:角色扮演|数据格式化|伪代码)/,
    /can(?:not|'t) continue (?:executing|with|performing)?\s*(?:roleplay|data formatting|pseudo-code)/i,
];

/**
 * 软模式：常见道歉 / 拒绝句式。必须同时满足「短回复 + 无 RP 对白/动作标记 +
 * 出现在回复开头 100 字符内」，避免把角色台词里的“对不起”误判。
 */
const REFUSAL_SOFT_PATTERNS = [
    // ---- English ----
    /i(?:'m| am) sorry,?\s*but\b/i,
    /i apologize,?\s*but\b/i,
    // “I'm sorry (that/but) I can't/won't…” 才是拒答；“I'm sorry I'm late” 这类日常道歉不命中
    /i(?:'m| am) sorry,?\s*(?:but\s+)?(?:i|that)\s+(?:can(?:'t|not)|cannot|can not|won't|will not|am unable|must (?:decline|refuse)|won't be able)/i,
    /i(?:'m| am) afraid (?:i|that|but)\b/i,
    /i (?:can(?:'t|not)|cannot|can not|am unable to|won't|will not) (?:help|assist|provide|fulfill|fulfil|comply|generate|create|write|continue|engage|do (?:that|so|this))\b/i,
    /i(?:'m| am) (?:not able|unable) to (?:help|assist|provide|comply|generate|fulfill|fulfil|continue|do)\b/i,
    /i must (?:decline|refuse|politely decline)\b/i,
    /that(?:'s| is) not (?:something )?(?:that )?i can (?:help|assist)(?: you)? with\b/i,
    /i won't be able to (?:help|assist|provide|generate|continue|comply)\b/i,
    /i(?:'m| am) not (?:comfortable|willing|permitted|allowed) (?:helping|with|to)\b/i,
    /i can(?:'t|not) (?:fulfill|fulfil) (?:that|this|such) request\b/i,
    /please (?:seek|consult|contact) (?:a |an |professional|local|appropriate|legal|emergency)/i,
    // ---- 中文 ----
    /抱歉[，,。\s]*(?:但|不过)?\s*我(?:无法|不能|没法|不便|没办法)/,
    /对不起[，,。\s]*(?:但|不过)?\s*我(?:无法|不能|没法|不便|没办法)/,
    /我(?:无法|不能|没法|不便)为?(?:你|您)?(?:提供|协助|帮助|生成|创作|继续|满足|处理|帮忙|完成|回答)/,
    /我(?:不能|无法)(?:帮助|协助|帮忙)(?:你|您)?/,
    /我(?:无法|不能)满足(?:你|您)?(?:的|这个|这种|该)?(?:要求|请求|需求)/,
    /恕(?:难从命|不奉陪|不能(?:奉陪|答应|帮忙|协助|提供))/,
    /(?:这|此)(?:超出了|超出|不在)我(?:的)?(?:能力|服务|职责|能力范围|服务范围|职责范围)/,
    /(?:违反|触犯)(?:了)?(?:相关)?(?:法律法规|法律|法规|平台规定|平台规则|政策规定|使用政策|政策准则)/,
    /还请(?:多多)?(?:谅解|理解|见谅|包涵)/,
    /建议(?:你|您)(?:寻求|咨询|联系)(?:专业|相关|当地|法律|急救)?(?:人士|机构|帮助|咨询)?/,
];

/**
 * 文本中是否包含明显的 RP 对白 / 动作标记（用于拒答软模式豁免）。
 */
function hasRoleplayMarkers(text) {
    if (/[“”‘’「」『』]/.test(text)) return true;                         // 中文引号
    if (/"[^"]*[\w\u4e00-\u9fff][^"]*"/.test(text)) return true;          // 英文引号对白
    if (/\*[^*\n]{2,}\*/.test(text)) return true;                         // *动作*
    if (/（[^）]{2,}）/.test(text)) return true;                          // （动作）
    if (/〔[^〕]{2,}〕/.test(text)) return true;                          // 〔动作〕
    return false;
}

/**
 * 解析用户自定义拒答句式：每行一个；/表达式/标志 按正则解析，纯文本按不区分大小写的子串匹配。
 * 非法行静默跳过。
 */
function compileCustomPatterns(raw) {
    return String(raw || '')
        .split(/\r?\n/)
        .map((line) => {
            const trimmed = line.trim();
            if (!trimmed) return null;
            try {
                const m = trimmed.match(/^\/(.+)\/([a-z]*)$/i);
                if (m) return new RegExp(m[1], m[2] || 'i');
                const escaped = trimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
                return new RegExp(escaped, 'i');
            } catch {
                return null;
            }
        })
        .filter(Boolean);
}

/**
 * 判断一条 AI 消息是否为模型拒答。
 * 返回命中的短语字符串（用于日志/提示），否则返回 null。
 */
function detectRefusal(message, settings) {
    if (!message) return null;
    const extra = message.extra || {};

    // 媒体 / 工具 / 附件 / 系统消息不参与拒答判定
    if (Array.isArray(extra.media) && extra.media.length > 0) return null;
    if (Array.isArray(extra.tool_invocations) && extra.tool_invocations.length > 0) return null;
    if (Array.isArray(extra.attachments) && extra.attachments.length > 0) return null;

    const raw = String(message.mes ?? '');
    if (/<(?:img|video|audio|iframe|object|embed)\b/i.test(raw)) return null;

    const text = normalizePlainText(raw);
    if (!text || text === '...' || text === '…') return null;

    const firstMatch = (patterns) => {
        for (const re of patterns) {
            const m = text.match(re);
            if (m) return m[0];
        }
        return null;
    };

    // 1) 强模式：AI 自述话术，命中即判
    const strongHit = firstMatch(REFUSAL_STRONG_PATTERNS);
    if (strongHit) return strongHit;

    // 2) 用户自定义句式（视为强模式，由用户自行负责）
    const customHit = firstMatch(compileCustomPatterns(settings.refusalCustomPatterns));
    if (customHit) return customHit;

    // 3) 软模式：短回复 + 无 RP 标记 + 句式出现在开头
    if (settings.refusalMaxLength > 0 && text.length > settings.refusalMaxLength) return null;
    if (hasRoleplayMarkers(text)) return null;

    const head = text.slice(0, 100);
    for (const re of REFUSAL_SOFT_PATTERNS) {
        const m = head.match(re);
        if (m) return m[0];
    }
    return null;
}

/**
 * 结合 fetch 信号、toast 信号和最终消息状态，判定本次生成是否失败、失败类型是否启用重试。
 * 返回失败对象（含 label），或 null（视为成功 / 不应重试）。
 */
function classifyFailure(settings) {
    const chat = getCtx().chat || [];

    // 1) 硬信号：HTTP 状态码 / 网络错误
    const hardSignal = watch.signals.find((s) => s.kind === 'http' || s.kind === 'network');
    if (hardSignal) {
        if (hardSignal.kind === 'http') {
            const status = hardSignal.status;
            if (status === 429) {
                return settings.retryOn429
                    ? { ...hardSignal, label: 'HTTP 429 · 请求过于频繁（限流）' }
                    : null;
            }
            if (status >= 500 && status < 600) {
                return settings.retryOn5xx
                    ? { ...hardSignal, label: `HTTP ${status} · 服务器错误` }
                    : null;
            }
            if (status === 403) {
                return settings.retryOn403
                    ? { ...hardSignal, label: 'HTTP 403 · 拒绝访问' }
                    : null;
            }
            if (status >= 400 && status < 500) {
                return settings.retryOn4xxOther
                    ? { ...hardSignal, label: `HTTP ${status} · 客户端错误` }
                    : null;
            }
            return null;
        }

        // 网络错误：客户端主动 abort 通常来自用户停止，交给 userStopped 拦截
        if (watch.userStopped || hardSignal.name === 'AbortError') return null;
        return settings.retryOnNetwork
            ? { ...hardSignal, label: '网络错误 · 连接中断 / 请求失败' }
            : null;
    }

    // 2) 看最后一条消息
    const last = chat[chat.length - 1];
    if (last && !last.is_user) {
        // 系统消息 / 旁白不是 AI 输出，不能据此判空；走到这里又没有硬信号，保守视为成功
        if (last.is_system || last.extra?.type === 'NARRATOR') {
            return null;
        }
        if (messageLooksEmpty(last, settings)) {
            return settings.retryOnEmpty
                ? { kind: 'empty', label: '空回复 · 请求成功但没有任何输出' }
                : null;
        }
        // 模型拒答（道歉 / 拒绝话术、跳出角色的“作为 AI”）
        const refusalPhrase = detectRefusal(last, settings);
        if (refusalPhrase) {
            return settings.retryOnRefusal
                ? {
                    kind: 'refusal',
                    label: `模型拒答 · 检测到拒答话术「${refusalPhrase.slice(0, 30)}」`,
                }
                : null;
        }
        // 有部分内容，但窗口内出现过错误 toast（流式输出到一半断开）
        const hasErrorToast = watch.signals.some((s) => s.kind === 'toast');
        if (hasErrorToast) {
            return settings.retryOnPartial
                ? { kind: 'partial', label: '流式中断 · 已有部分内容' }
                : null;
        }
        return null; // 有完整输出 → 成功
    }

    // 3) 最后一条是 user 消息 / chat 为空（未产生 AI 回复）
    const toastTexts = watch.signals
        .filter((s) => s.kind === 'toast')
        .map((s) => s.text)
        .join(' ');
    if (toastTexts) {
        // 本地酒馆后端没启动（Start.bat 没开），重试无意义
        if (/server is running|cannot be reached|ST Server/i.test(toastTexts)) return null;
        return settings.retryOnNetwork
            ? { kind: 'toast', label: '生成报错 · 未产生回复', text: toastTexts }
            : null;
    }

    // 3b) 零输出兜底：生成流程完整结束，却没有任何新 AI 消息留下
    // 关键时序：user 消息在 STARTED 之后才写入 chat（Generate 内部），所以普通发送
    //   零输出时条数 = startLen + 1（只有 user 消息），regenerate 失败时条数 < startLen。
    // 用 fetchCount 区分「确实发过请求但零输出」与「群聊成员为 0 / 前置拦截，根本没发请求」。
    const currentLen = chat.length;
    const lastIsUser = !!last && last.is_user === true;
    const requestWasSent = watch.fetchCount > 0;
    const missingAfterUser = currentLen === watch.startLen + 1 && lastIsUser && requestWasSent;
    const missingAfterRegenerate = currentLen <= watch.startLen && (lastIsUser || !last);
    if (missingAfterUser || missingAfterRegenerate) {
        return settings.retryOnEmpty
            ? { kind: 'empty', label: '空回复 · 生成结束但没有任何输出' }
            : null;
    }

    return null;
}

// ============================ 退避计算 ============================

function parseRetryAfter(value) {
    if (!value) return null;
    const text = String(value).trim();
    if (/^\d+$/.test(text)) return Number(text); // 秒
    const date = new Date(text);
    if (!Number.isNaN(date.getTime())) return Math.max(0, (date.getTime() - Date.now()) / 1000);
    return null;
}

function computeDelay(attemptIndex, failure, settings) {
    let delay = settings.baseDelay * Math.pow(settings.factor, attemptIndex);
    delay = Math.min(settings.maxDelay, delay);

    if (failure.kind === 'http' && failure.status === 429) {
        const retryAfter = settings.respectRetryAfter ? parseRetryAfter(failure.retryAfter) : null;
        if (retryAfter != null && Number.isFinite(retryAfter)) {
            delay = Math.max(delay, Math.min(retryAfter, 300));
        } else {
            delay = Math.max(delay, 10); // 429 没有 Retry-After 时至少等 10 秒
        }
    }

    if (settings.jitter) {
        delay *= 0.8 + Math.random() * 0.4; // ±20%
    }
    return Math.max(0.5, Math.round(delay * 10) / 10);
}

// ============================ 状态机事件 ============================

function deactivate() {
    clearTimers();
    hideBar();
    watch.attempt = 0;
    watch.active = false;
    watch.signals = [];
    watch.userStopped = false;
    retryChainUntil = 0;
}

function onGenerationStarted(type, params, dryRun) {
    // 静默生成（后台摘要 / 函数调用）与模拟生成完全不介入
    if (dryRun) return;
    if (type === 'quiet') return;

    // impersonate（替我发言）/ continue（续写，含自动续写）是独立功能，失败不由本插件接管；
    // 若此时正在等待重试，说明用户在操作，取消挂起的重试并退出监控
    if (type === 'impersonate' || type === 'continue') {
        if (retryTimer) cancelPending();
        deactivate();
        return;
    }

    const inRetryChain = ownRetry || Date.now() < retryChainUntil;
    const hadPending = !!retryTimer;
    ownRetry = false;
    cancelPending();

    // 等待重试期间被用户手动发起的生成打断 → 旧重试链作废
    if (!inRetryChain && hadPending) {
        retryChainUntil = 0;
    }

    const settings = getSettings();
    const ctx = getCtx();
    const previousAttempt = inRetryChain ? watch.attempt : 0;

    watch.token += 1;
    watch.evaluatedToken = -1;
    watch.confirmToken = -1;
    watch.active = false;
    watch.signals = [];
    watch.fetchCount = 0;
    watch.userStopped = false;
    watch.type = type ?? null;
    watch.attempt = previousAttempt;

    if (!settings.enabled) return;
    if (ctx.groupId && !settings.retryOnGroup) return;

    watch.active = true;
    watch.chatId = currentChatId();
    watch.startLen = ctx.chat?.length ?? 0;
    watch.startAnchor = (ctx.chat && ctx.chat.length) ? ctx.chat[ctx.chat.length - 1] : null;
    // 续期宽限：群聊 wrapper、工具调用递归会在链内再次触发 STARTED
    if (inRetryChain) retryChainUntil = Date.now() + 30000;

    console.debug(LOG_PREFIX, `生成开始（type=${type ?? 'normal'}，失败链已重试 ${watch.attempt} 次）`);
}

function onGenerationStopped() {
    if (watch.active) {
        watch.userStopped = true;
    }
    // 用户主动停止，挂起的重试一并取消
    if (retryTimer) {
        cancelPending();
        deactivate();
    }
}

function onGenerationEnded() {
    if (!watch.active) return;
    const token = watch.token;
    // STOPPED 与 ENDED 在同一个同步调用栈（stopGeneration 先 hideStopButton 再 emit STOPPED），
    // 250ms 足够接收 STOPPED 并让消息块完成 finalize；硬信号失败立即判定，软判定还会二次确认
    setTimeout(() => evaluate(token, false), 250);
}

function onChatChanged() {
    if (retryTimer) {
        cancelPending();
        console.debug(LOG_PREFIX, '聊天已切换，取消自动重Roll');
    }
    watch.active = false;
}

function evaluate(token, reconfirmed) {
    if (!watch.active || token !== watch.token || watch.evaluatedToken === token) return;

    const settings = getSettings();
    if (!settings.enabled) {
        watch.evaluatedToken = token;
        deactivate();
        return;
    }

    // 用户主动停止 → 绝不重试
    if (watch.userStopped) {
        watch.evaluatedToken = token;
        console.debug(LOG_PREFIX, '用户主动停止，不自动重Roll');
        deactivate();
        return;
    }

    const failure = classifyFailure(settings);

    // 软判定（空回复 / 仅 toast 报错、未产生回复）：消息可能尚未完成 finalize 落盘，
    // 延迟 350ms 复读一次，仍为空才重试——这是防止误伤成功内容的关键双保险
    if (failure && (failure.kind === 'empty' || failure.kind === 'toast') && !reconfirmed) {
        if (watch.confirmToken !== token) {
            watch.confirmToken = token;
            console.debug(LOG_PREFIX, '疑似空回复 / 无输出，等待 350ms 二次确认…');
            setTimeout(() => evaluate(token, true), 350);
        }
        return;
    }

    watch.evaluatedToken = token;

    if (!failure) {
        console.debug(LOG_PREFIX, '本次生成成功，重置重试计数');
        deactivate();
        return;
    }

    // 聊天已被切换
    if (currentChatId() !== watch.chatId) {
        deactivate();
        return;
    }

    if (watch.attempt >= settings.maxRetries) {
        console.warn(LOG_PREFIX, `已达到最大重试次数（${settings.maxRetries}），停止自动重Roll：${failure.label}`);
        if (settings.showToast && window.toastr) {
            window.toastr.error(
                `失败原因：${failure.label}。已达到最大重试次数（${settings.maxRetries}），请手动处理。`,
                '自动重Roll 已停止',
                { timeOut: 8000, preventDuplicates: true },
            );
        }
        deactivate();
        return;
    }

    scheduleRetry(failure);
}

// ============================ 调度重试 ============================

function scheduleRetry(failure) {
    const settings = getSettings();
    const nextAttempt = watch.attempt + 1;
    watch.attempt = nextAttempt;

    const delaySec = computeDelay(nextAttempt - 1, failure, settings);

    console.warn(LOG_PREFIX, `将在 ${delaySec}s 后自动重Roll（第 ${nextAttempt}/${settings.maxRetries} 次）：${failure.label}`);

    showBar(failure, nextAttempt, settings.maxRetries, delaySec);

    if (settings.showToast && window.toastr) {
        window.toastr.info(
            `${delaySec} 秒后自动重新生成（第 ${nextAttempt}/${settings.maxRetries} 次）。可在下方状态条取消。`,
            `生成失败：${failure.label}`,
            { timeOut: Math.min(8000, Math.max(2000, delaySec * 1000)), preventDuplicates: true },
        );
    }

    let remaining = delaySec;
    updateCountdown(remaining);
    countdownTimer = setInterval(() => {
        remaining = Math.max(0, Math.round((remaining - 0.2) * 10) / 10);
        updateCountdown(remaining);
    }, 200);

    retryTimer = setTimeout(() => {
        clearTimers();
        hideBar();
        doRetry();
    }, delaySec * 1000);
}

function doRetry() {
    const settings = getSettings();
    if (!settings.enabled) return;

    if (currentChatId() !== watch.chatId) {
        console.debug(LOG_PREFIX, '聊天已切换，放弃本次重试');
        deactivate();
        return;
    }
    if (isGenerating()) {
        console.debug(LOG_PREFIX, '酒馆正在生成中，跳过本次重试');
        deactivate();
        return;
    }

    const ctx = getCtx();
    const chat = ctx.chat || [];
    const last = chat[chat.length - 1];
    if (!last || typeof last.is_user === 'undefined') {
        console.debug(LOG_PREFIX, '没有可重试的消息，放弃');
        deactivate();
        return;
    }

    const watchType = watch.type;

    // 触发前最终复核：此刻仍判定为失败才重试。
    // 若等待期间成功内容落盘（迟到的流式 finalize、用户手动处理等），立即放弃，绝不覆盖。
    const recheck = classifyFailure(settings);
    if (!recheck) {
        console.log(LOG_PREFIX, '触发前复核：已检测到正常输出，取消本次重试（不误伤成功内容）');
        deactivate();
        return;
    }
    // swipe 重试要求最后一条仍是 AI 消息（右滑目标必须是 AI 消息）
    if (watchType === 'swipe' && last.is_user) {
        console.debug(LOG_PREFIX, 'swipe 目标消息不存在，放弃');
        deactivate();
        return;
    }

    ownRetry = true;
    retryChainUntil = Date.now() + 30000;

    // 兜底：如果重试没有触发新的生成（按钮被拦截、正在编辑消息等），复位标记，
    // 不主动清链——迟到的 STARTED 仍可按重试链处理，用户手动发起的新生成也会自然重置
    setTimeout(() => {
        if (ownRetry) {
            ownRetry = false;
            console.warn(LOG_PREFIX, '重试未被酒馆响应（未检测到新的生成开始），本次自动重试作废');
            deactivate();
        }
    }, 3000);

    try {
        if (watchType === 'swipe') {
            // 与酒馆官方 auto_swipe 完全同款：向右滑再生成一个新候选，
            // 失败的空槽与旧回复都由酒馆原生 swipes 机制保留，不会丢内容
            console.log(LOG_PREFIX, '已触发「右滑重试」（swipe right）');
            const result = ctx.swipe.to(null, 'right', {
                source: 'auto_swipe',
                repeated: true,
                forceMesId: chat.length - 1,
            });
            if (result && typeof result.catch === 'function') {
                result.catch((err) => {
                    ownRetry = false;
                    console.error(LOG_PREFIX, '右滑重试调用失败', err);
                });
            }
            return;
        }

        // 单聊：点击「重新生成」（与用户手动点击完全一致，可正确处理空消息残留 / 未产生消息两种情况）
        // 群聊：同一按钮走原生 regenerateGroup()
        const btn = document.getElementById('option_regenerate');
        if (btn) {
            window.jQuery(btn).trigger('click');
            console.log(LOG_PREFIX, '已触发「重新生成」');
        } else if (ctx.groupId) {
            throw new Error('群聊中找不到 #option_regenerate 按钮');
        } else {
            // 兜底：按钮不存在时直接调用原生 Generate('regenerate')
            ctx.generate('regenerate', {}).catch((err) => {
                ownRetry = false;
                retryChainUntil = 0;
                console.error(LOG_PREFIX, '重试调用失败', err);
                deactivate();
            });
        }
    } catch (err) {
        ownRetry = false;
        retryChainUntil = 0;
        console.error(LOG_PREFIX, '重试触发失败', err);
        deactivate();
    }
}

// ============================ 倒计时状态条 ============================

function ensureBar() {
    let bar = document.getElementById('arerr_bar');
    if (bar) return bar;

    bar = document.createElement('div');
    bar.id = 'arerr_bar';
    bar.className = 'arerr-bar';
    bar.innerHTML = `
        <span class="arerr-bar-spin" aria-hidden="true">↻</span>
        <div class="arerr-bar-body">
            <div class="arerr-bar-title">自动重Roll · 第 <b id="arerr_bar_attempt"></b> / <b id="arerr_bar_max"></b> 次</div>
            <div class="arerr-bar-sub"><span id="arerr_bar_reason"></span> · <span id="arerr_bar_countdown"></span> 秒后自动重新生成</div>
        </div>
        <button type="button" id="arerr_bar_now" class="arerr-btn arerr-btn-primary">立即重试</button>
        <button type="button" id="arerr_bar_cancel" class="arerr-btn">取消</button>
    `;
    document.body.appendChild(bar);

    bar.querySelector('#arerr_bar_now').addEventListener('click', () => {
        clearTimers();
        hideBar();
        doRetry();
    });
    bar.querySelector('#arerr_bar_cancel').addEventListener('click', () => {
        cancelPending();
        deactivate();
        if (getSettings().showToast && window.toastr) {
            window.toastr.info('已取消自动重Roll', '自动重Roll', { timeOut: 2000 });
        }
    });
    return bar;
}

function showBar(failure, attempt, max, delay) {
    const bar = ensureBar();
    bar.querySelector('#arerr_bar_attempt').textContent = String(attempt);
    bar.querySelector('#arerr_bar_max').textContent = String(max);
    bar.querySelector('#arerr_bar_reason').textContent = failure.label;
    bar.querySelector('#arerr_bar_countdown').textContent = String(Math.ceil(delay));
    bar.style.display = 'flex';
}

function updateCountdown(seconds) {
    const el = document.getElementById('arerr_bar_countdown');
    if (el) el.textContent = String(Math.ceil(seconds));
}

function hideBar() {
    const bar = document.getElementById('arerr_bar');
    if (bar) bar.style.display = 'none';
}

function clearTimers() {
    if (retryTimer) {
        clearTimeout(retryTimer);
        retryTimer = null;
    }
    if (countdownTimer) {
        clearInterval(countdownTimer);
        countdownTimer = null;
    }
}

function cancelPending() {
    clearTimers();
    hideBar();
}

// ============================ 设置面板 ============================

const CHECKBOX_FIELDS = [
    ['enabled', '启用自动重Roll（总开关）'],
    ['retryOn429', '429 限流（自动延长等待，读取 Retry-After）'],
    ['retryOn5xx', '5xx 服务器错误（500 / 502 / 503 / 504 等）'],
    ['retryOn403', '403 拒绝访问'],
    ['retryOn4xxOther', '其他 4xx 错误（400 / 401 / 404 等，通常重试无效）'],
    ['retryOnNetwork', '网络错误 / 连接中断 / 请求失败'],
    ['retryOnEmpty', '空回复（请求成功但没有任何输出）'],
    ['retryOnRefusal', '模型拒答（道歉 / 拒绝话术、"作为 AI"等跳角色回复）'],
    ['retryOnPartial', '流式中断且已有部分内容（会删掉残文重新生成）'],
    ['reasonOnlyAsEmpty', '只有推理没有正文，也算空回复'],
    ['retryOnGroup', '群聊中也启用自动重Roll'],
    ['respectRetryAfter', '429 时优先遵守服务器给出的 Retry-After 等待时间'],
    ['jitter', '等待时间随机抖动 ±20%（避免限流时撞车）'],
    ['showToast', '显示通知提示'],
];

const NUMBER_FIELDS = [
    ['maxRetries', '最大重试次数', 0, 20, 1],
    ['baseDelay', '首次等待（秒）', 0.5, 120, 0.5],
    ['factor', '退避倍率（每次等待 ×该值）', 1, 10, 0.5],
    ['maxDelay', '单次等待上限（秒）', 5, 600, 1],
    ['refusalMaxLength', '拒答软模式：仅短于该长度的回复生效（字符，0=不限）', 0, 5000, 50],
];

function buildSettingsPanel() {
    const host = document.getElementById('extensions_settings2')
        || document.getElementById('extensions_settings');
    if (!host) return false;
    if (document.getElementById(`${MODULE_NAME}_container`)) return true;

    const settings = getSettings();

    const checkboxHtml = CHECKBOX_FIELDS.map(([key, label]) => `
        <label class="checkbox_label arerr-check" title="${label}">
            <input type="checkbox" id="arerr_${key}" ${settings[key] ? 'checked' : ''}>
            <span>${label}</span>
        </label>
    `).join('');

    const numberHtml = NUMBER_FIELDS.map(([key, label, min, max, step]) => `
        <div class="arerr-field" title="${label}">
            <label for="arerr_${key}">${label}</label>
            <input type="number" class="text_pole" id="arerr_${key}"
                value="${settings[key]}" min="${min}" max="${max}" step="${step}">
        </div>
    `).join('');

    const container = document.createElement('div');
    container.id = `${MODULE_NAME}_container`;
    container.className = 'extension_container';
    container.innerHTML = `
        <div class="inline-drawer">
            <div class="inline-drawer-toggle inline-drawer-header">
                <b>↻ 自动重Roll（Auto Re-roll）</b>
                <div class="inline-drawer-icon fa-solid fa-circle-chevron-down down"></div>
            </div>
            <div class="inline-drawer-content">
                <div class="arerr-hint">
                    生成请求失败（500 / 503 / 429 / 403、网络中断、空回复等）导致酒馆停止且没有输出时，
                    自动重新执行「重新生成」：首次约 1 秒即重试，之后按 1→2→4→8→16 秒退避。
                    空回复会二次确认、触发前还会再复核，绝不会把成功内容误判重发。
                    用户主动点停止不会触发；等待期间发消息或切换聊天会自动取消。
                </div>
                <div class="arerr-section-title">触发条件</div>
                <div class="arerr-grid">${checkboxHtml}</div>
                <div class="arerr-section-title">重试节奏（指数退避）</div>
                <div class="arerr-fields">${numberHtml}</div>
                <div class="arerr-section-title">自定义拒答句式（可选，每行一个）</div>
                <textarea id="arerr_refusalCustomPatterns" class="text_pole arerr-textarea" rows="4"
                    placeholder="纯文本按不区分大小写的子串匹配；写成 /表达式/标志 则按正则解析，例如：&#10;作为人工智能&#10;/I cannot (?:help|assist)/i"></textarea>
                <div class="arerr-hint">
                    命中自定义句式会直接判定为拒答（不看长度与引号），请谨慎填写；留空则只使用内置库。
                    内置软模式要求「短回复 + 无对白引号/动作标记 + 句式出现在开头」，避免误伤角色台词。
                </div>
                <div class="arerr-actions">
                    <input type="button" class="menu_button arerr-reset" value="恢复默认设置">
                </div>
            </div>
        </div>
    `;
    host.appendChild(container);

    for (const [key] of CHECKBOX_FIELDS) {
        container.querySelector(`#arerr_${key}`).addEventListener('change', (e) => {
            getSettings()[key] = !!e.target.checked;
            saveSettings();
        });
    }

    for (const [key, , min, max, step] of NUMBER_FIELDS) {
        const input = container.querySelector(`#arerr_${key}`);
        input.addEventListener('change', () => {
            let value = Number(input.value);
            if (!Number.isFinite(value)) value = DEFAULT_SETTINGS[key];
            value = Math.min(max, Math.max(min, value));
            const decimals = String(step).includes('.') ? 1 : 0;
            value = Number(value.toFixed(decimals));
            input.value = String(value);
            getSettings()[key] = value;
            saveSettings();
        });
    }

    const patternsInput = container.querySelector('#arerr_refusalCustomPatterns');
    patternsInput.value = settings.refusalCustomPatterns ?? '';
    patternsInput.addEventListener('input', () => {
        getSettings().refusalCustomPatterns = patternsInput.value;
        saveSettings();
    });

    container.querySelector('.arerr-reset').addEventListener('click', () => {
        const s = getCtx().extensionSettings[MODULE_NAME];
        for (const [key, defaultValue] of Object.entries(DEFAULT_SETTINGS)) {
            s[key] = defaultValue;
        }
        s._version = SETTINGS_VERSION;
        saveSettings();
        for (const [key] of CHECKBOX_FIELDS) {
            container.querySelector(`#arerr_${key}`).checked = !!s[key];
        }
        for (const [key] of NUMBER_FIELDS) {
            container.querySelector(`#arerr_${key}`).value = String(s[key]);
        }
        container.querySelector('#arerr_refusalCustomPatterns').value = s.refusalCustomPatterns;
        if (window.toastr) {
            window.toastr.success('已恢复默认设置', '自动重Roll', { timeOut: 2000 });
        }
    });

    return true;
}

// ============================ 初始化 ============================

function init() {
    const settings = getSettings();
    if (migrateSettings(settings)) saveSettings();
    installFetchHook();
    installToastHook();

    eventSource.on(event_types.GENERATION_STARTED, onGenerationStarted);
    eventSource.on(event_types.GENERATION_STOPPED, onGenerationStopped);
    eventSource.on(event_types.GENERATION_ENDED, onGenerationEnded);
    eventSource.on(event_types.CHAT_CHANGED, onChatChanged);

    if (!buildSettingsPanel()) {
        let tries = 0;
        const timer = setInterval(() => {
            if (buildSettingsPanel() || ++tries > 40) clearInterval(timer);
        }, 500);
    }

    console.log(`${LOG_PREFIX} v${MODULE_VERSION} 已加载`);
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
} else {
    init();
}
