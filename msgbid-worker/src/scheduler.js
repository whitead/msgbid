export class SchedulerDurableObject {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    // Initialize instance variables
    this.batch = [];
    this.pendingRequests = [];
    this.processing = false;

    this.N = parseInt(env.N) || 5;
    this.timeout = parseInt(env.TIMEOUT) || 5000;
    this.accumulate = parseInt(env.ACCUMULATE_BAL) || 0;
    this.start = parseInt(env.START_BAL) || 10;
    this.max_bal = parseInt(env.MAX_BAL) || 100;

    // No need to load data into memory at startup
  }

  async fetch(request) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;

      if (request.method === 'OPTIONS') {
        return this.handleOptions(request);
      }

      switch (pathname) {
        case '/register':
          if (request.method === 'PUT') {
            return await this.handleRegister(request);
          }
          break;
        case '/messages':
          if (request.method === 'POST') {
            return await this.handleSendMessage(request);
          } else if (request.method === 'GET') {
            return await this.handleReplayMessages(request);
          }
          break;
        case '/balance':
          if (request.method === 'GET') {
            return await this.handleGetBalance(request);
          }
          break;
        case '/clients':
          if (request.method === 'GET') {
            return await this.handleListClients(request);
          }
          break;
        case '/delete':
          if (request.method === 'GET') {
            return await this.handleDelete(request);
          }
          break;
    
      }
      return new Response('Not found', { status: 404 });
    } catch (error) {
      console.error('Error handling request:', error);
      return new Response('Internal Server Error', { status: 500 });
    }
  }

  handleOptions(request) {
    const headers = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, PUT, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Client-Token',
    };
    return new Response(null, { headers });
  }

  async handleRegister(request) {
    if (request.method !== 'PUT') {
      return new Response('Method Not Allowed', { status: 405 });
    }

    const { name } = await this.parseRequestBody(request);

    if (!name) {
      return new Response('Name is required', { status: 400 });
    }

    const token = this.generateToken();
    const balance = this.start;

    await this.state.storage.put(`balance:${token}`, balance);
    await this.state.storage.put(`name:${token}`, name);

    const response = { token, balance, name };
    return new Response(JSON.stringify(response), {
      headers: this.getResponseHeaders(),
    });
  }

  async handleGetBalance(request) {
    if (request.method !== 'GET') {
      return new Response('Method Not Allowed', { status: 405 });
    }

    const token = request.headers.get('X-Client-Token');

    if (!token) {
      return new Response('Missing token header', { status: 400 });
    }

    const [balance, name] = await Promise.all([
      this.state.storage.get(`balance:${token}`),
      this.state.storage.get(`name:${token}`),
    ]);

    if (balance == null) {
      return new Response('Invalid token', { status: 400 });
    }

    const response = { balance, name };
    return new Response(JSON.stringify(response), {
      headers: this.getResponseHeaders(),
    });
  }

  async handleListClients(request) {
    if (request.method !== 'GET') {
      return new Response('Method Not Allowed', { status: 405 });
    }

    const adminToken = request.headers.get('Authorization');
    const url = new URL(request.url);
    const page = parseInt(url.searchParams.get('page')) || 1;
    const pageSize = parseInt(url.searchParams.get('pageSize')) || 10;

    if (adminToken !== `Bearer ${this.env.ADMIN_TOKEN}`) {
      return new Response('Unauthorized', { status: 401 });
    }

    //abuse the balances to get list of tokens
    const balances = await this.state.storage.list({ prefix: 'balance:' });
    const tokens = Array.from(balances.keys()).map((key) => key.slice('balance:'.length));

    const totalClients = tokens.length;
    const totalPages = Math.ceil(totalClients / pageSize);
    const start = (page - 1) * pageSize;
    const end = start + pageSize;
    const pageTokens = tokens.slice(start, end);

    const clientDataKeys = [];
    for (const token of pageTokens) {
      clientDataKeys.push(`balance:${token}`, `name:${token}`);
    }


    const clientData = await this.state.storage.get(clientDataKeys);
    const clients = {};

    for (const token of pageTokens) {
      const balance = clientData.get(`balance:${token}`);
      const name = clientData.get(`name:${token}`);
      clients[token] = { balance, name };
    }

    const response = {
      clients,
      pagination: {
        page,
        pageSize,
        totalClients,
        totalPages,
      },
    };

    return new Response(JSON.stringify(response), {
      headers: this.getResponseHeaders(),
    });
  }

  async handleReplayMessages(request) {
    if (request.method !== 'GET') {
      return new Response('Method Not Allowed', { status: 405 });
    }

    const url = new URL(request.url);
    const end = url.searchParams.get('end');
    const limit = parseInt(url.searchParams.get('limit')) || 10;

    const listOptions = {
      prefix: 'message:',
      reverse: true,
      limit,
      end,
    };

    const messagesMap = await this.state.storage.list(listOptions);

    const messages = [];
    let lastKey = null;
    for (const [key, value] of messagesMap.entries()) {
      messages.push(value);
      lastKey = key;
    }

    const hasMore = messages.length === limit;
    const response = {
      messages,
      next: hasMore ? lastKey : null,
    };

    return new Response(JSON.stringify(response), {
      headers: this.getResponseHeaders(),
    });
  }

  async handleSendMessage(request) {
    if (request.method !== 'POST') {
      return new Response('Method Not Allowed', { status: 405 });
    }

    const token = request.headers.get('X-Client-Token');

    if (!token) {
      return new Response('Missing token header', { status: 400 });
    }

    const { message, bid } = await this.parseRequestBody(request);

    if (!message || bid == null) {
      return new Response('Missing parameters', { status: 400 });
    }

    if (typeof bid !== 'number' || bid <= 0) {
      return new Response('Invalid bid amount', { status: 400 });
    }

    const clientBalance = await this.state.storage.get(`balance:${token}`);

    if (clientBalance == null || clientBalance < bid) {
      return new Response('Invalid token or insufficient balance', { status: 400 });
    }

    // Add bid to the batch
    this.batch.push({ token, message, bid });

    // Start timeout if first bid in batch
    if (this.batch.length === 1) {
      this.state.storage.setAlarm(Date.now() + this.timeout);
    }

    // Process batch if enough bids
    if (this.batch.length >= this.N) {
      await this.state.blockConcurrencyWhile(async () => {
        await this.processBatch();
      });
    } else {
      return new Promise((resolve) => {
        this.pendingRequests.push({ token, resolve });
      });
    }

    // This point should not be reached
    return new Response('Unexpected error', { status: 500 });
  }

  async processBatch() {
    if (this.processing) return;
    this.processing = true;

    await this.state.blockConcurrencyWhile(async () => {
      await this.state.storage.deleteAlarm();

      // Filter out duplicate bids, keeping only the highest bid per token
      const uniqueBidsMap = new Map();
      for (const bid of this.batch) {
        const existingBid = uniqueBidsMap.get(bid.token);
        if (!existingBid || bid.bid > existingBid.bid) {
          uniqueBidsMap.set(bid.token, bid);
        }
      }
      const uniqueBids = Array.from(uniqueBidsMap.values());
      const nBids = uniqueBids.length;

      // Get balances and names for all unique bidders
      const tokens = uniqueBids.map((bid) => bid.token);
      const balanceKeys = tokens.map((token) => `balance:${token}`);
      const nameKeys = tokens.map((token) => `name:${token}`);

      const [balancesObj, namesObj] = await Promise.all([
        this.state.storage.get(balanceKeys),
        this.state.storage.get(nameKeys),
      ]);

      const balances = new Map();
      const names = new Map();
      for (let i = 0; i < tokens.length; i++) {
        const token = tokens[i];
        const balanceKey = balanceKeys[i];
        const nameKey = nameKeys[i];
        balances.set(token, balancesObj.get(balanceKey));
        names.set(token, namesObj.get(nameKey));
      }

      // Sort unique bids to find highest and second-highest
      uniqueBids.sort((a, b) => b.bid - a.bid);
      const highestBidder = uniqueBids[0];
      const secondHighestBid = uniqueBids[1]?.bid || 0;
      const sumBid = uniqueBids.reduce((acc, bid) => acc + bid.bid, 0);

      // Deduct the second-highest bid from the highest bidder's balance
      const highestToken = highestBidder.token;
      let highestBalance = balances.get(highestToken);

      highestBalance -= secondHighestBid;
      if (highestBalance < 0) highestBalance = 0;

      balances.set(highestToken, highestBalance);

      // Accumulate balances for all unique bidders
      for (const bid of uniqueBids) {
        const token = bid.token;
        if (token !== highestToken) {
          let balance = balances.get(token);
          balance = Math.min(this.max_bal, balance + this.accumulate);
          balances.set(token, balance);
        }
      }

      // Write updated balances back to storage
      const updatedBalances = {};
      for (const [token, balance] of balances) {
        updatedBalances[`balance:${token}`] = balance;
      }
      await this.state.storage.put(updatedBalances);

      // Store the accepted message
      const acceptedMessage = {
        message: highestBidder.message,
        bidderToken: highestToken,
        bidderName: names.get(highestToken),
        timestamp: new Date().toISOString(),
      };

      // Use timestamp and random suffix to ensure uniqueness
      const messageId = `message:${Date.now()}-${Math.random().toString(36).substring(2, 7)}`;
      await this.state.storage.put(messageId, acceptedMessage);

      // Compute statistics
      const stats = {
        winBid: secondHighestBid,
        sumBid: sumBid,
        nBids,
      };

      // Prepare responses for all clients
      const responses = {};
      for (const bid of this.batch) {
        const token = bid.token;
        const balance = balances.get(token);
        const name = names.get(token);
        responses[token] = {
          message: highestBidder.message,
          balance,
          name,
          status: token === highestToken ? 'accepted' : 'rejected',
          stats,
        };
      }

      // Resolve all pending requests
      for (const pending of this.pendingRequests) {
        const responseData = responses[pending.token];
        pending.resolve(
          new Response(JSON.stringify(responseData), {
            headers: this.getResponseHeaders(),
          })
        );
      }

      // Reset state
      this.batch = [];
      this.pendingRequests = [];
      this.processing = false;
    });
  }

  async handleDelete(request) {
    if (request.method !== 'GET') {
      return new Response('Method Not Allowed', { status: 405 });
    }
  
    const adminToken = request.headers.get('Authorization');
  
    if (adminToken !== `Bearer ${this.env.ADMIN_TOKEN}`) {
      return new Response('Unauthorized', { status: 401 });
    }
  
    await this.state.blockConcurrencyWhile(async () => {
      // Delete any pending alarm
      await this.state.storage.deleteAlarm();
  
      // List and delete all entries
      const allKeys = [];
      
      // Get all balance entries
      let balanceList = await this.state.storage.list({ prefix: 'balance:' });
      for (const key of balanceList.keys()) {
        allKeys.push(key);
      }
  
      // Get all name entries
      let nameList = await this.state.storage.list({ prefix: 'name:' });
      for (const key of nameList.keys()) {
        allKeys.push(key);
      }
  
      // Get all message entries
      let messageList = await this.state.storage.list({ prefix: 'message:' });
      for (const key of messageList.keys()) {
        allKeys.push(key);
      }
  
      // Delete all entries
      await this.state.storage.delete(allKeys);
  
      // Reset instance variables
      this.batch = [];
      this.pendingRequests = [];
      this.processing = false;
    });
  
    return new Response(JSON.stringify({
      success: true,
      message: 'All data has been deleted'
    }), {
      headers: this.getResponseHeaders()
    });
  }

  async alarm() {
    if (this.batch.length > 0) {
      await this.state.blockConcurrencyWhile(async () => {
        await this.processBatch();
      });
    }
  }

  // Utility methods
  generateToken() {
    const randomBytes = new Uint8Array(12);
    crypto.getRandomValues(randomBytes);
    const base64Token = btoa(String.fromCharCode(...randomBytes))
      .replace(/[+/]/g, '') // Remove + and / characters
      .slice(0, 16);

    return `${base64Token}`;
  }

  getResponseHeaders() {
    return {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Expose-Headers': 'X-Client-Token',
    };
  }

  async parseRequestBody(request) {
    const contentType = request.headers.get('Content-Type') || '';
    if (contentType.includes('application/json')) {
      return await request.json();
    } else {
      throw new Error('Unsupported Content-Type');
    }
  }
}
