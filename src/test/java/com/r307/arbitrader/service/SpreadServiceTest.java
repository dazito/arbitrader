package com.r307.arbitrader.service;

import com.r307.arbitrader.ExchangeBuilder;
import com.r307.arbitrader.service.cache.ExchangeFeeCache;
import com.r307.arbitrader.service.model.Spread;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.knowm.xchange.Exchange;
import org.knowm.xchange.currency.CurrencyPair;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.math.BigDecimal;

import static org.junit.Assert.*;

public class SpreadServiceTest {
    private Exchange longExchange;
    private Exchange shortExchange;

    private SpreadService spreadService;

    @Before
    public void setUp() throws IOException {
        MockitoAnnotations.initMocks(this);
        final TickerService tickerServiceMock = Mockito.mock(TickerService.class);
        final ExchangeFeeCache exchangeFeeCache = Mockito.mock(ExchangeFeeCache.class);

        longExchange = new ExchangeBuilder("Long", CurrencyPair.BTC_USD)
            .withExchangeMetaData()
            .build();
        shortExchange = new ExchangeBuilder("Short", CurrencyPair.BTC_USD)
            .withExchangeMetaData()
            .build();

        spreadService = new SpreadService(tickerServiceMock, exchangeFeeCache);
    }

    @Test
    public void testSummary() {
        Spread spread = new Spread(
            CurrencyPair.BTC_USD,
            longExchange,
            shortExchange,
            null,
            null,
            BigDecimal.valueOf(-0.005),
            BigDecimal.valueOf(0.005));

        spreadService.publish(spread);
        spreadService.summary();
    }

    @Test
    public void testComputeEntrySpread() {
        BigDecimal longPrice = new BigDecimal("1000");
        BigDecimal shortPrice = new BigDecimal("1010");
        BigDecimal shortFee = new BigDecimal("0.0026");
        BigDecimal longFee = new BigDecimal("0.005");

        final BigDecimal entrySpread = spreadService.computeEntrySpread(longPrice, longFee, shortPrice, shortFee);
        assertEquals(new BigDecimal("0.00236219"), entrySpread);
    }

    @Test
    public void testComputeExitSpread() {
        BigDecimal longPrice = new BigDecimal("1000");
        BigDecimal shortPrice = new BigDecimal("1010");
        BigDecimal shortFee = new BigDecimal("0.0026");
        BigDecimal longFee = new BigDecimal("0.005");

        final BigDecimal exitSpread = spreadService.computeExitSpread(longPrice, longFee, shortPrice, shortFee);
        assertEquals(new BigDecimal("-0.01771457"), exitSpread);

    }
}
