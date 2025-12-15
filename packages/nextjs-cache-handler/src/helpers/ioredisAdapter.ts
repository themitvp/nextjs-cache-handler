import type { RedisClientType } from "@redis/client";
import type { Redis } from "ioredis";

/**
 * Adapter to make an ioredis client compatible with the interface expected by createRedisHandler.
 *
 * @param client - The ioredis client instance.
 * @returns A proxy that implements the subset of RedisClientType used by this library.
 */
export function ioredisAdapter(client: Redis): RedisClientType {
  return new Proxy(client, {
    get(target, prop, receiver) {
      if (prop === "isReady") {
        return target.status === "ready";
      }

      if (prop === "hScan") {
        return async (
          key: string,
          cursor: string,
          options?: { COUNT?: number },
        ) => {
          let result: [string, string[]];

          if (options?.COUNT) {
            result = await target.hscan(key, cursor, "COUNT", options.COUNT);
          } else {
            result = await target.hscan(key, cursor);
          }

          const [newCursor, items] = result;

          const entries = [];
          for (let i = 0; i < items.length; i += 2) {
            entries.push({ field: items[i], value: items[i + 1] });
          }

          return {
            cursor: newCursor,
            entries,
          };
        };
      }

      if (prop === "hDel") {
        return async (key: string, fields: string | string[]) => {
          const args = Array.isArray(fields) ? fields : [fields];
          if (args.length === 0) return 0;
          return target.hdel(key, ...args);
        };
      }

      if (prop === "unlink") {
        return async (keys: string | string[]) => {
          const args = Array.isArray(keys) ? keys : [keys];
          if (args.length === 0) return 0;
          return target.unlink(...args);
        };
      }

      if (prop === "set") {
        return async (key: string, value: string, options?: any) => {
          const args: (string | number)[] = [key, value];
          if (options) {
            if (options.EXAT) {
              args.push("EXAT", options.EXAT);
            } else if (options.PXAT) {
              args.push("PXAT", options.PXAT);
            } else if (options.EX) {
              args.push("EX", options.EX);
            } else if (options.PX) {
              args.push("PX", options.PX);
            } else if (options.KEEPTTL) {
              args.push("KEEPTTL");
            }
            // Add other options if necessary
          }
          // Cast to a generic signature to avoid overload mismatch issues with dynamic args
          const setFn = target.set as unknown as (
            key: string,
            value: string | number,
            ...args: (string | number)[]
          ) => Promise<string | null>;

          return setFn(
            args[0] as string,
            args[1] as string | number,
            ...args.slice(2),
          );
        };
      }

      if (prop === "hmGet") {
        return async (key: string, fields: string | string[]) => {
          const args = Array.isArray(fields) ? fields : [fields];
          if (args.length === 0) return [];
          return target.hmget(key, ...args);
        };
      }

      // Handle camelCase to lowercase mapping for other methods
      if (typeof prop === "string") {
        // Special case for expireAt -> expireat
        if (prop === "expireAt") return target.expireat.bind(target);

        // hSet -> hset
        if (prop === "hSet") return target.hset.bind(target);

        // hExists -> hexists
        if (prop === "hExists") return target.hexists.bind(target);

        // Default fallback to lowercase if exists
        const lowerProp = prop.toLowerCase();
        if (
          lowerProp in target &&
          typeof (target as any)[lowerProp] === "function"
        ) {
          return (target as any)[lowerProp].bind(target);
        }
      }

      return Reflect.get(target, prop, receiver);
    },
  }) as unknown as RedisClientType;
}
