#property strict

input string SupabaseUrl = "https://YOUR_PROJECT.supabase.co";
input string SupabaseKey = "YOUR_SERVICE_ROLE_KEY";
input int UpdateSeconds = 10;
input int MaxSymbols = 50;
input bool UseOpenOperations = true;
input string SymbolsCsv = "PETR4,VALE3";
input bool SendDailyClose = true;

string g_headers = "";

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
   string updatedAt = TimeToString(TimeCurrent(), TIME_DATE|TIME_SECONDS);
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
   string dateStr = TimeToString(rate.time, TIME_DATE);
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

   Print("HTTP error ", code, " for ", url, " body=", body);
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
   string url = SupabaseUrl + "/rest/v1/acoes_asset?select=id&ticker=eq." + symbol;
   string response = HttpGet(url);
   if (response == "")
      return 0;

   return ParseSingleId(response);
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
   Print("HTTP error ", code, " for ", url);
   return "";
}

int ParseSingleId(string json)
{
   // TODO: parse JSON properly.
   return 0;
}

int ParseAssetIdsFromOperations(string json, int &ids[], int limit)
{
   // TODO: parse JSON properly.
   return 0;
}

int ParseTickers(string json, string &out[], int limit)
{
   // TODO: parse JSON properly.
   return 0;
}
