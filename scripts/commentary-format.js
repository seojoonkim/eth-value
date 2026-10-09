// Normalize LLM commentary into exactly 3 paragraphs joined by "|||".
// Returns { text, repaired } or null when the text is too short/garbled to fix safely.
function normalizeParagraphs(raw) {
    if (!raw || typeof raw !== 'string') return null;
    let t = raw.trim().replace(/^```[a-z]*\s*|```$/g, '').trim();
    let parts = t.split(/\s*\|{3}\s*/).map(x => x.trim()).filter(Boolean);
    if (parts.length === 3) return { text: parts.join('|||'), repaired: false };
    // Other separators the model sometimes uses: blank lines, "||", single newlines.
    for (const re of [/\n\s*\n+/, /\s*\|\|\s*/, /\n+/]) {
        const p = t.split(re).map(x => x.replace(/\|+/g, ' ').trim()).filter(x => x.length > 20);
        if (p.length === 3) return { text: p.join('|||'), repaired: true };
    }
    // Last resort: split sentences into three balanced groups.
    const flat = t.replace(/\|+/g, ' ').replace(/\s+/g, ' ').trim();
    const sents = flat.match(/[^.!?。！？]+[.!?。！？]+["'”’)]*\s*/g) || [];
    if (sents.length < 6) return null;
    const n = sents.length, a = Math.round(n / 3), b = Math.round(2 * n / 3);
    return { text: [sents.slice(0, a), sents.slice(a, b), sents.slice(b)].map(g => g.join('').trim()).join('|||'), repaired: true };
}
module.exports = { normalizeParagraphs };
