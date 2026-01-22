#property strict

input string SupabaseUrl = "https://YOUR_PROJECT.supabase.co";
input string SupabaseKey = "YOUR_SERVICE_ROLE_KEY";
input int UpdateSeconds = 10;
input int MaxSymbols = 50;
input bool UseOpenOperations = true;
input string SymbolsCsv = "PETR4,VALE3";
input bool SendDailyClose = true;

string g_headers = "";
string g_cache_symbols[];
int g_cache_ids[];

string FormatIso8601Utc()
{
   string ts = TimeToString(TimeGMT(), TIME_DATE | TIME_SECONDS);
   ts = StringReplace(ts, ".", "-");
   ts = StringReplace(ts, " ", "T");
   return ts + "Z";
}

string FormatIsoDate(datetime value)
{
   string dateStr = TimeToString(value, TIME_DATE);
   return StringReplace(dateStr, ".", "-");
}

int CacheGetId(string symbol)
{
   int size = ArraySize(g_cache_symbols);
   for (int i = 0; i < size; i++)
   {
      if (g_cache_symbols[i] == symbol)
      {
         return g_cache_ids[i];
      }
   }
   return 0;
}

void CachePutId(string symbol, int assetId)
{
   int size = ArraySize(g_cache_symbols);
   for (int i = 0; i < size; i++)
   {
      if (g_cache_symbols[i] == symbol)
      {
         g_cache_ids[i] = assetId;
         return;
      }
   }
   ArrayResize(g_cache_symbols, size + 1);
   ArrayResize(g_cache_ids, size + 1);
   g_cache_symbols[size] = symbol;
   g_cache_ids[size] = assetId;
}

int OnInit()
{
   g_headers =
      "Content-Type: application/json\r\n"
      "apikey: " + SupabaseKey + "\r\n"
      "Authorization: Bearer " + SupabaseKey + "\r\n"
      "Prefer: resolution=merge-duplicates\r\n";

   EventSetTimer(UpdateSeconds);
   return INIT_SUCCEEDED;
}

void OnDeinit(const int reason)
{
   EventKillTimer();
}

void OnTimer()
{
   string symbols[];
   int count = 0;

   if (UseOpenOperations)
   {
      count = FetchOpenOperationSymbols(symbols, MaxSymbols);
   }
   else
   {
      count = ParseSymbolsCsv(SymbolsCsv, symbols, MaxSymbols);
   }

   if (count <= 0)
   {
      Print("No symbols to update.");
      return;
   }

   for (int i = 0; i < count; i++)
   {
      string symbol = symbols[i];
      int assetId = FetchAssetId(symbol);
      if (assetId <= 0)
      {
         Print("Asset not found in Supabase: ", symbol);
         continue;
      }

      double price = GetLastPrice(symbol);
      if (price <= 0.0)
      {
         Print("No price for symbol: ", symbol);
         continue;
      }

      if (!UpsertQuoteLive(assetId, price))
      {
         Print("Failed to upsert live quote for ", symbol);
      }

      if (SendDailyClose)
      {
         MqlRates daily;
         if (GetLastDailyRate(symbol, daily))
         {
            UpsertQuoteDaily(assetId, daily);
         }
      }
   }
}

int ParseSymbolsCsv(string csv, string &out[], int limit)
{
   string parts[];
   int n = StringSplit(csv, ',', parts);
   int count = 0;
   for (int i = 0; i < n && count < limit; i++)
   {
      string s = StringTrim(parts[i]);
      if (s != "")
      {
         out[count++] = s;
      }
   }
   return count;
}

double GetLastPrice(string symbol)
{
   MqlTick tick;
   if (!SymbolInfoTick(symbol, tick))
   {
      return 0.0;
   }
   if (tick.last > 0.0)
      return tick.last;
   if (tick.bid > 0.0)
      return tick.bid;
   if (tick.ask > 0.0)
      return tick.ask;
   return 0.0;
}

bool GetLastDailyRate(string symbol, MqlRates &rate)
{
   MqlRates rates[];
   int copied = CopyRates(symbol, PERIOD_D1, 0, 1, rates);
   if (copied <= 0)
   {
      return false;
   }
   rate = rates[0];
   return true;
}

bool UpsertQuoteLive(int assetId, double price)
{
   string endpoint = SupabaseUrl + "/rest/v1/cotacoes_quotelive?on_conflict=asset_id";
   string updatedAt = FormatIso8601Utc();
   string body = "{"
      "\"asset_id\":" + IntegerToString(assetId) + ","
      "\"price\":" + DoubleToString(price, 6) + ","
      "\"updated_at\":\"" + updatedAt + "\""
      "}";

   return HttpPost(endpoint, body);
}

bool UpsertQuoteDaily(int assetId, MqlRates rate)
{
   string endpoint = SupabaseUrl + "/rest/v1/cotacoes_quotedaily?on_conflict=asset_id,date";
   string dateStr = FormatIsoDate(rate.time);
   string body = "{"
      "\"asset_id\":" + IntegerToString(assetId) + ","
      "\"date\":\"" + dateStr + "\","
      "\"open\":" + DoubleToString(rate.open, 6) + ","
      "\"high\":" + DoubleToString(rate.high, 6) + ","
      "\"low\":" + DoubleToString(rate.low, 6) + ","
      "\"close\":" + DoubleToString(rate.close, 6) + ","
      "\"is_provisional\":false"
      "}";

   return HttpPost(endpoint, body);
}

bool HttpPost(string url, string body)
{
   char data[];
   StringToCharArray(body, data);

   char result[];
   string result_headers;
   int timeout = 10000;
   int code = WebRequest("POST", url, g_headers, timeout, data, result, result_headers);
   if (code >= 200 && code < 300)
   {
      return true;
   }

   string response = CharArrayToString(result);
   Print("HTTP error ", code, " for ", url, " body=", body, " response=", response, " headers=", result_headers);
   return false;
}

// FetchOpenOperationSymbols and FetchAssetId are placeholders.
// Implement JSON parsing or add a JSON helper library.
int FetchOpenOperationSymbols(string &out[], int limit)
{
   string url = SupabaseUrl + "/rest/v1/operacoes_operation?status=eq.open&select=sell_asset_id,buy_asset_id";
   string response = HttpGet(url);
   if (response == "")
      return 0;

   int ids[];
   int idCount = ParseAssetIdsFromOperations(response, ids, limit * 2);
   if (idCount <= 0)
      return 0;

   return FetchTickersByAssetIds(ids, idCount, out, limit);
}

int FetchAssetId(string symbol)
{
   int cached = CacheGetId(symbol);
   if (cached > 0)
      return cached;

   string url = SupabaseUrl + "/rest/v1/acoes_asset?select=id&ticker=eq." + symbol;
   string response = HttpGet(url);
   if (response == "")
      return 0;

   int assetId = ParseSingleId(response);
   if (assetId > 0)
      CachePutId(symbol, assetId);
   return assetId;
}

int FetchTickersByAssetIds(int &ids[], int idCount, string &out[], int limit)
{
   string list = "";
   for (int i = 0; i < idCount; i++)
   {
      if (i > 0)
         list += ",";
      list += IntegerToString(ids[i]);
   }

   string url = SupabaseUrl + "/rest/v1/acoes_asset?select=id,ticker&id=in.(" + list + ")";
   string response = HttpGet(url);
   if (response == "")
      return 0;

   return ParseTickers(response, out, limit);
}

string HttpGet(string url)
{
   char result[];
   string result_headers;
   int timeout = 10000;
   int code = WebRequest("GET", url, g_headers, timeout, NULL, result, result_headers);
   if (code >= 200 && code < 300)
   {
      return CharArrayToString(result);
   }
   string response = CharArrayToString(result);
   Print("HTTP error ", code, " for ", url, " response=", response, " headers=", result_headers);
   return "";
}

bool ExtractIntValue(string json, string key, int start, int &value, int &nextPos)
{
   int keyPos = StringFind(json, key, start);
   if (keyPos < 0)
      return false;

   int colon = StringFind(json, ":", keyPos + StringLen(key));
   if (colon < 0)
      return false;

   int i = colon + 1;
   int len = StringLen(json);
   while (i < len && StringGetCharacter(json, i) <= ' ')
      i++;

   bool neg = false;
   if (i < len && StringGetCharacter(json, i) == '-')
   {
      neg = true;
      i++;
   }

   int startDigits = i;
   int result = 0;
   while (i < len)
   {
      ushort ch = StringGetCharacter(json, i);
      if (ch < '0' || ch > '9')
         break;
      result = result * 10 + (int)(ch - '0');
      i++;
   }

   if (i == startDigits)
      return false;

   value = neg ? -result : result;
   nextPos = i;
   return true;
}

bool ExtractStringValue(string json, string key, int start, string &value, int &nextPos)
{
   int keyPos = StringFind(json, key, start);
   if (keyPos < 0)
      return false;

   int colon = StringFind(json, ":", keyPos + StringLen(key));
   if (colon < 0)
      return false;

   int i = colon + 1;
   int len = StringLen(json);
   while (i < len && StringGetCharacter(json, i) <= ' ')
      i++;

   if (i >= len || StringGetCharacter(json, i) != '"')
      return false;

   int startQuote = i + 1;
   int endQuote = StringFind(json, "\"", startQuote);
   if (endQuote < 0)
      return false;

   value = StringSubstr(json, startQuote, endQuote - startQuote);
   nextPos = endQuote + 1;
   return true;
}

bool AddUniqueInt(int &values[], int &count, int limit, int value)
{
   for (int i = 0; i < count; i++)
   {
      if (values[i] == value)
         return false;
   }
   if (count >= limit)
      return false;

   ArrayResize(values, count + 1);
   values[count] = value;
   count++;
   return true;
}

bool AddUniqueString(string &values[], int &count, int limit, string value)
{
   for (int i = 0; i < count; i++)
   {
      if (values[i] == value)
         return false;
   }
   if (count >= limit)
      return false;

   ArrayResize(values, count + 1);
   values[count] = value;
   count++;
   return true;
}

int ParseSingleId(string json)
{
   int value = 0;
   int nextPos = 0;
   if (ExtractIntValue(json, "\"id\"", 0, value, nextPos))
      return value;
   return 0;
}

int ParseAssetIdsFromOperations(string json, int &ids[], int limit)
{
   int count = 0;
   ArrayResize(ids, 0);

   int pos = 0;
   int value = 0;
   int nextPos = 0;
   while (ExtractIntValue(json, "\"sell_asset_id\"", pos, value, nextPos))
   {
      AddUniqueInt(ids, count, limit, value);
      pos = nextPos;
   }

   pos = 0;
   while (ExtractIntValue(json, "\"buy_asset_id\"", pos, value, nextPos))
   {
      AddUniqueInt(ids, count, limit, value);
      pos = nextPos;
   }

   return count;
}

int ParseTickers(string json, string &out[], int limit)
{
   int count = 0;
   ArrayResize(out, 0);

   int pos = 0;
   string ticker = "";
   int nextPos = 0;
   while (ExtractStringValue(json, "\"ticker\"", pos, ticker, nextPos))
   {
      AddUniqueString(out, count, limit, ticker);
      pos = nextPos;
   }

   return count;
}
