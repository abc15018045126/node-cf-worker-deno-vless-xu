import { connect } from 'cloudflare:sockets';

const defaultUserIDs = '2ea73714-138e-4cc7-8cab-d7caf476d51b';
const proxyip = '';

async function handleVlessProxy(request, env) {
	try {
		const validUserIDs = (env.ID || defaultUserIDs).split(',');

		const bufferToUUID = (buffer) => {
			if (!buffer || buffer.length !== 16) return null;
			let uuid = '';
			for (let i = 0; i < 16; i++) {
				uuid += buffer[i].toString(16).padStart(2, '0');
				if (i === 3 || i === 5 || i === 7 || i === 9) {
					uuid += '-';
				}
			}
			return uuid;
		};

		let currentproxyip = env.proxyip || proxyip;
		const url = new URL(request.url);
		if (url.pathname.toLowerCase().startsWith('/proxyip=')) {
			const newProxyIP = url.pathname.substring(9).trim();
			if (newProxyIP) {
				currentproxyip = newProxyIP;
			}
		}

		if (request.headers.get('Upgrade')?.toLowerCase() !== 'websocket') {
			return new Response('Worker is running. Expecting a WebSocket upgrade.', { status: 200 });
		}

		const [client, webSocket] = Object.values(new WebSocketPair());
		const effectiveProxyIP = currentproxyip;
		const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';

		let remoteSocket = null;
		let isClosed = false;

		const close = (reason) => {
			if (isClosed) return;
			isClosed = true;
			try { webSocket.close(); } catch (e) {}
			try { remoteSocket?.close(); } catch (e) {}
		};

		const handleTCPOutbound = async (host, port, initialPayload) => {
			const connectAndWrite = async (address, p) => {
				const socket = connect({ hostname: address, port: p });
				if (initialPayload.byteLength > 0) {
					const writer = socket.writable.getWriter();
					await writer.write(initialPayload);
					writer.releaseLock();
				}
				return socket;
			};
	
			const pipeRemoteToWS = async (socket, retryFn) => {
				remoteSocket = socket;
				let hasIncomingData = false;
				await remoteSocket.readable.pipeTo(new WritableStream({
					write: (chunk) => {
						hasIncomingData = true;
						if (webSocket.readyState === WebSocket.OPEN) webSocket.send(chunk);
					},
					close: () => {},
					abort: () => {},
				})).catch(() => {});
				if (isClosed) return;
				if (!hasIncomingData && retryFn) {
					await retryFn();
				} else {
					close();
				}
			};
	
			const retryWithProxy = effectiveProxyIP ? async () => {
				try {
					const proxySocket = await connectAndWrite(effectiveProxyIP, port);
					await pipeRemoteToWS(proxySocket, null);
				} catch (proxyErr) {
					close();
				}
			} : null;
	
			try {
				const destinationSocket = await connectAndWrite(host, port);
				await pipeRemoteToWS(destinationSocket, retryWithProxy);
			} catch {
				if (retryWithProxy) {
					await retryWithProxy();
				} else {
					close();
				}
			}
		};

		const onMessage = async (event) => {
			if (isClosed) return;
			try {
				const msg = new Uint8Array(event.data);
				if (remoteSocket) {
					const writer = remoteSocket.writable.getWriter();
					await writer.write(msg);
					writer.releaseLock();
					return;
				}
				if (msg.byteLength < 19) throw new Error();
				const clientUUIDBuffer = msg.slice(1, 17);
				const clientUUID = bufferToUUID(clientUUIDBuffer);
				if (!clientUUID || !validUserIDs.includes(clientUUID)) {
					throw new Error();
				}
				let offset = 17;
				const addonLen = msg[offset++];
				offset += addonLen;
				if (msg.byteLength < offset + 4) throw new Error();
				const command = msg[offset++];
				if (command !== 1) throw new Error();
				const dataView = new DataView(msg.buffer);
				const port = dataView.getUint16(offset, false);
				offset += 2;
				const addrType = msg[offset++];
				let host;
				switch (addrType) {
					case 1: host = msg.slice(offset, offset + 4).join('.'); offset += 4; break;
					case 2: const len = msg[offset++]; host = new TextDecoder().decode(msg.slice(offset, offset + len)); offset += len; break;
					case 3: const ipv6 = []; for (let i = 0; i < 8; i++) ipv6.push(dataView.getUint16(offset + i * 2, false).toString(16)); host = `[${ipv6.join(':')}]`; offset += 16; break;
					default: throw new Error();
				}
				webSocket.send(new Uint8Array([msg[0], 0]));
				const initialPayload = msg.slice(offset);
				await handleTCPOutbound(host, port, initialPayload);
			} catch {
				close();
			}
		};
	
		const processEarlyData = async () => {
			if (!earlyDataHeader) return;
			try {
				const earlyData = atob(earlyDataHeader.replace(/-/g, '+').replace(/_/g, '/'));
				const buffer = new Uint8Array(earlyData.length);
				for (let i = 0; i < earlyData.length; i++) buffer[i] = earlyData.charCodeAt(i);
				await onMessage({ data: buffer.buffer });
			} catch {
				close();
			}
		};
	
		webSocket.accept();
		webSocket.addEventListener('close', () => close());
		webSocket.addEventListener('error', () => close());
		webSocket.addEventListener('message', onMessage);
		processEarlyData();
		
		return new Response(null, { status: 101, webSocket: client });
	} catch {
		return new Response('Internal Server Error', { status: 500 });
	}
}

async function handleWssRelay(request, env) {
    try {
        const url = new URL(request.url);
        const proxyURL = url.pathname.substring(9).trim();
        const upgradeHeader = request.headers.get('Upgrade');
        if (!upgradeHeader || upgradeHeader.toLowerCase() !== 'websocket') {
            return new Response('Expected a WebSocket upgrade request.', { status: 426 });
        }
        const backendUrl = new URL(proxyURL);
        backendUrl.protocol = 'https';
        const proxyRequest = new Request(backendUrl, {
            headers: { 'Upgrade': 'websocket' },
            redirect: 'manual',
        });
        const response = await fetch(proxyRequest);
        const backendSocket = response.webSocket;
        if (response.status !== 101 || !backendSocket) {
            return new Response('', { status: 502 });
        }
        const [clientSocket, serverSocket] = Object.values(new WebSocketPair());
        serverSocket.accept();
        backendSocket.accept();
        let isClosed = false;
        const closeSockets = () => {
            if (isClosed) return;
            isClosed = true;
            try { serverSocket.close(1000); } catch (e) {}
            try { backendSocket.close(1000); } catch (e) {}
        };
        serverSocket.addEventListener('message', event => {
            try { if (!isClosed) backendSocket.send(event.data); } catch (e) { closeSockets(); }
        });
        backendSocket.addEventListener('message', event => {
            try { if (!isClosed) serverSocket.send(event.data); } catch (e) { closeSockets(); }
        });
        serverSocket.addEventListener('close', () => closeSockets());
        backendSocket.addEventListener('close', () => closeSockets());
        serverSocket.addEventListener('error', () => closeSockets());
        backendSocket.addEventListener('error', () => closeSockets());
        return new Response(null, { status: 101, webSocket: clientSocket });
    } catch {
        return new Response('Internal Server Error', { status: 500 });
    }
}

export default {
	fetch: (request, env) => {
		if (new URL(request.url).pathname.toLowerCase().startsWith('/proxyip=ws')) {
			return handleWssRelay(request, env);
		} else {
			return handleVlessProxy(request, env);
		}
	},
};
