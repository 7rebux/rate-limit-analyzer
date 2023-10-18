class RateLimitQueue {
  constructor(limitPerMinute) {
    this.limitPerMinute = limitPerMinute;
    this.queue = [];
    this.currentRequests = 0;
  }

  async add(task) {
    return new Promise((resolve, reject) => {
      this.queue.push({ task, resolve, reject });
      this.process();
    });
  }

  async process() {
    if (this.queue.length === 0 || this.currentRequests >= this.limitPerMinute) {
      return;
    }

    const { task, resolve, reject } = this.queue.shift();
    this.currentRequests++;

    try {
      const result = await task();
      resolve(result);
    } catch (error) {
      reject(error);
    } finally {
      this.currentRequests--;
      setTimeout(() => this.process(), 60000 / this.limitPerMinute);
    }
  }
}

export const rateLimitQueue = new RateLimitQueue(100);