import { connect } from 'cloudflare:sockets';

const proxyip = '';
const utf8Decoder = new TextDecoder("utf-8");

async function hp(rq, en) {
  try {
    let cp = en.proxyip || proxyip;
    const urlStr = rq.url;
    const pathIndex = urlStr.indexOf('/', 8);
    if (pathIndex > -1 && urlStr.substring(pathIndex).toLowerCase().startsWith('/proxyip=')) {
      const np = urlStr.substring(pathIndex + 9).trim();
      if (np) cp = np;
    }

    if (rq.headers.get('Upgrade')?.toLowerCase() !== 'websocket')
      return new Response('Worker is running. Expecting a WebSocket upgrade.', { status: 200 });

    const [c1, c2] = Object.values(new WebSocketPair());
    let rs = null, udpProcessor = null, ic = 0;
    
    const cl = () => { if (ic) return; ic = 1; try { c2.close(); } catch {} try { rs?.close(); } catch {} };

    const ht = async (h, p, y) => {
      const cw = async (ad, p) => {
        const so = connect({ hostname: ad, port: p });
        if (y?.length) {
          const writer = so.writable.getWriter();
          await writer.write(y);
          writer.releaseLock();
        }
        return so;
      };
      const pr = async (so, rf) => {
        rs = so;
        let hi = 0;
        await rs.readable.pipeTo(new WritableStream({ write: ck => { hi = 1; if (c2.readyState === 1) c2.send(ck); } })).catch(() => {});
        if (ic) return;
        if (!hi && rf) await rf(); else cl();
      };
      const rw = cp ? async () => { try { await pr(await cw(cp, p), null); } catch { cl(); } } : null;
      try { await pr(await cw(h, p), rw); } catch { if (rw) await rw(); else cl(); }
    };

    const om = async ev => {
      if (ic) return;
      let ms = ev.data;
      if (!(ms instanceof Uint8Array)) ms = new Uint8Array(ms);

      if (rs) {
        const writer = rs.writable.getWriter();
        await writer.write(ms);
        writer.releaseLock();
        return;
      }
      if (udpProcessor) {
        udpProcessor(ms).catch(cl);
        return;
      }
      
      if (ms.length < 4) return cl();
      let of = 18 + ms[17];
      if (ms.length < of + 4) return cl();

      const command = ms[of];
      const dv = new DataView(ms.buffer, ms.byteOffset, ms.byteLength);
      const po = dv.getUint16(of + 1, false);
      of += 3;

      if (command === 1) { // TCP
        let ho;
        const ad_type = ms[of++];
        if (ad_type === 1) {
          if (ms.length < of + 4) return cl();
          ho = `${ms[of]}.${ms[of+1]}.${ms[of+2]}.${ms[of+3]}`; of += 4;
        } else if (ad_type === 2) {
          if (ms.length < of + 1) return cl();
          const ln = ms[of++];
          if (ms.length < of + ln) return cl();
          ho = utf8Decoder.decode(ms.subarray(of, of + ln)); of += ln;
        } else if (ad_type === 3) {
          if (ms.length < of + 16) return cl();
          let s = '';
          for (let i = 0; i < 8; i++) { s += dv.getUint16(of + i * 2, false).toString(16); if (i < 7) s += ':'; }
          ho = `[${s}]`; of += 16;
        } else { return cl(); }
        c2.send(new Uint8Array([ms[0], 0]));
        await ht(ho, po, ms.subarray(of));
      } else if (command === 2) { // UDP
        if (po !== 53) return cl();
        const ad_type = ms[of++];
        if (ad_type === 1) of += 4;
        else if (ad_type === 2) { if (ms.length < of + 1) return cl(); const ln = ms[of++]; of += ln; }
        else if (ad_type === 3) of += 16;
        else return cl();
        
        udpProcessor = await setupUDPProcessor(c2, new Uint8Array([ms[0], 0]));
        udpProcessor(ms.subarray(of)).catch(cl);
      } else {
        cl();
      }
    };

    const eh = rq.headers.get('sec-websocket-protocol');
    if (eh) {
      try {
        await om({ data: Uint8Array.from(atob(eh.replace(/-/g, '+').replace(/_/g, '/')), c => c.charCodeAt(0)).buffer });
      } catch { cl(); }
    }
    c2.accept();
    c2.addEventListener('close', cl);
    c2.addEventListener('error', cl);
    c2.addEventListener('message', om);
    return new Response(null, { status: 101, webSocket: c1 });
  } catch {
    return new Response('Internal Server Error', { status: 500 });
  }
}

async function setupUDPProcessor(webSocket, nlessResponseHeader) {
  let isNlessHeaderSent = false;
  return async function processChunk(chunk) {
    for (let index = 0; index < chunk.byteLength;) {
      if (chunk.byteLength < index + 2) break;
      const udpPakcetLength = new DataView(chunk.buffer, chunk.byteOffset + index).getUint16(0);
      const dataEnd = index + 2 + udpPakcetLength;
      if (chunk.byteLength < dataEnd) break;
      const udpData = chunk.subarray(index + 2, dataEnd);
      index = dataEnd;

      const resp = await fetch('https://1.1.1.1/dns-query', {
        method: 'POST',
        headers: { 'content-type': 'application/dns-message' },
        body: udpData,
      });
      const dnsQueryResult = await resp.arrayBuffer();
      if (webSocket.readyState !== 1) continue;
      
      const udpSize = dnsQueryResult.byteLength;
      const udpSizeBuffer = new Uint8Array([(udpSize >> 8) & 0xff, udpSize & 0xff]);
      const dnsQueryUint8 = new Uint8Array(dnsQueryResult);
      let dataToSend;
      
      if (isNlessHeaderSent) {
        dataToSend = new Uint8Array(2 + udpSize);
        dataToSend.set(udpSizeBuffer);
        dataToSend.set(dnsQueryUint8, 2);
      } else {
        dataToSend = new Uint8Array(nlessResponseHeader.length + 2 + udpSize);
        dataToSend.set(nlessResponseHeader);
        dataToSend.set(udpSizeBuffer, nlessResponseHeader.length);
        dataToSend.set(dnsQueryUint8, nlessResponseHeader.length + 2);
        isNlessHeaderSent = true;
      }
      webSocket.send(dataToSend);
    }
  };
}

export default { fetch: hp };