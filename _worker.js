import { connect } from 'cloudflare:sockets';

const id = '2ea73714-138e-4cc7-8cab-d7caf476d51b';
const proxyip = '';

async function hp(rq, en) {
	try {
		const vu = (en.ID || id).split(',');
		const bi = b => b && b.length === 16 ? [...b].map((x, i) => x.toString(16).padStart(2, '0') + ([3,5,7,9].includes(i) ? '-' : '')).join('') : null;
		let cp = en.proxyip || proxyip;
		const ur = new URL(rq.url);
		if (ur.pathname.toLowerCase().startsWith('/proxyip=')) {
			const np = ur.pathname.substring(9).trim();
			if (np) cp = np;
		}
		if (rq.headers.get('Upgrade')?.toLowerCase() !== 'websocket') return new Response('Worker is running. Expecting a WebSocket upgrade.', { status: 200 });
		const [c1, c2] = Object.values(new WebSocketPair());
		const eh = rq.headers.get('sec-websocket-protocol') || '';
		let rs = null;
		let ic = false;
		const cl = () => { if (ic) return; ic = true; try { c2.close(); } catch {} try { rs?.close(); } catch {} };
		const wt = async (wr, da) => { const w = wr.getWriter(); await w.write(da); w.releaseLock(); };
		const ht = async (h, p, y) => {
			const cw = async (ad, p) => {
				const so = connect({ hostname: ad, port: p });
				if (y && y.byteLength > 0) await wt(so.writable, y);
				return so;
			};
			const pr = async (so, rf) => {
				rs = so;
				let hi = false;
				await rs.readable.pipeTo(new WritableStream({
					write: ck => { hi = true; if (c2.readyState === WebSocket.OPEN) c2.send(ck); },
					close: () => {}, abort: () => {}
				})).catch(() => {});
				if (ic) return;
				if (!hi && rf) await rf(); else cl();
			};
			const rw = cp ? async () => { try { const ps = await cw(cp, p); await pr(ps, null); } catch { cl(); } } : null;
			try { const ds = await cw(h, p); await pr(ds, rw); }
			catch { if (rw) await rw(); else cl(); }
		};
		const om = async ev => {
			if (ic) return;
			try {
				const ms = new Uint8Array(ev.data);
				if (rs) { await wt(rs.writable, ms); return; }
				if (ms.byteLength < 19) throw new Error();
				const ci = bi(ms.slice(1, 17));
				if (!ci || !vu.includes(ci)) throw new Error();
				let of = 17;
				const al = ms[of++];
				of += al;
				if (ms.byteLength < of + 4) throw new Error();
				const cm = ms[of++];
				if (cm !== 1) throw new Error();
				const dv = new DataView(ms.buffer);
				const po = dv.getUint16(of, false);
				of += 2;
				const at = ms[of++];
				let ho;
				switch (at) {
					case 1:
						ho = `${ms[of]}.${ms[of+1]}.${ms[of+2]}.${ms[of+3]}`;
						of += 4;
						break;
					case 2: {
						const ln = ms[of++];
						ho = new TextDecoder().decode(ms.slice(of, of + ln));
						of += ln;
						break;
					}
					case 3: {
						const sg = Array.from({ length: 8 }, (_, i) => dv.getUint16(of + i * 2, false).toString(16));
						ho = `[${sg.join(':')}]`;
						of += 16;
						break;
					}
					default: throw new Error();
				}
				c2.send(new Uint8Array([ms[0], 0]));
				await ht(ho, po, ms.slice(of));
			} catch { cl(); }
		};
		if (eh) {
			try {
				const ed = atob(eh.replace(/-/g, '+').replace(/_/g, '/'));
				const bf = Uint8Array.from(ed, c => c.charCodeAt(0));
				await om({ data: bf.buffer });
			} catch { cl(); }
		}
		c2.accept();
		c2.addEventListener('close', cl);
		c2.addEventListener('error', cl);
		c2.addEventListener('message', om);
		return new Response(null, { status: 101, webSocket: c1 });
	} catch { return new Response('Internal Server Error', { status: 500 }); }
}

export default { fetch: hp };
