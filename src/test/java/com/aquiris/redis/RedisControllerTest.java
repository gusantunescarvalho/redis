package com.aquiris.redis;

import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RedisControllerTest extends AbstractTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
    }

    /**
     * Test creation of a new key/value pair.
     * @throws Exception
     */
    @Test
    public void t01_putKeyValueNumber1Test() throws Exception {

        String uri = "/key1";

        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.put(uri)
                .contentType(MediaType.TEXT_PLAIN)
                .content("value1")).andReturn();

        int status = mvcResult.getResponse().getStatus();
        assertEquals(200, status);
        String content = mvcResult.getResponse().getContentAsString();
        assertEquals("OK", content);
    }

    /**
     * Test get the value of the new key/value pair.
     * @throws Exception
     */
    @Test
    public void t02_getKeyValueNumber1Test() throws Exception {

        String uri = "/key1";

        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get(uri)).andReturn();

        int status = mvcResult.getResponse().getStatus();
        assertEquals(200, status);
        String content = mvcResult.getResponse().getContentAsString();
        assertEquals("value1", content);
    }

    /**
     * Test creation of another key/value pair.
     * @throws Exception
     */
    @Test
    public void t03_putKeyValueNumber2Test() throws Exception {

        String uri = "/key2";

        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.put(uri)
                .contentType(MediaType.TEXT_PLAIN)
                .content("value2")).andReturn();

        int status = mvcResult.getResponse().getStatus();
        assertEquals(200, status);
        String content = mvcResult.getResponse().getContentAsString();
        assertEquals("OK", content);
    }

    /**
     * Test get the newest key/value pair.
     * @throws Exception
     */
    @Test
    public void t04_getKeyValueNumber2Test() throws Exception {

        String uri = "/key2";

        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get(uri)).andReturn();

        int status = mvcResult.getResponse().getStatus();
        assertEquals(200, status);
        String content = mvcResult.getResponse().getContentAsString();
        assertEquals("value2", content);
    }

    /**
     * Test get the total number of keys.
     * @throws Exception
     */
    @Test
    public void t05_getDBSizeTest() throws Exception {

        String uri = "/";

        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get(uri)).andReturn();

        int status = mvcResult.getResponse().getStatus();
        assertEquals(200, status);
        String content = mvcResult.getResponse().getContentAsString();
        assertEquals("2", content);
    }

    /**
     * Test deletion of a key.
     * @throws Exception
     */
    @Test
    public void t06_deleteKeyValueNumber1Test() throws Exception {

        String uri = "/key1";

        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.delete(uri)).andReturn();

        int status = mvcResult.getResponse().getStatus();
        assertEquals(200, status);
        String content = mvcResult.getResponse().getContentAsString();
        assertEquals("OK", content);
    }

    /**
     * Test trying to get an inexisting key.
     * @throws Exception
     */
    @Test
    public void t07_getKeyValueNumber1AfterDeleteTest() throws Exception {

        String uri = "/key1";

        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get(uri)).andReturn();

        int status = mvcResult.getResponse().getStatus();
        assertEquals(200, status);
        String content = mvcResult.getResponse().getContentAsString();
        assertEquals("(nil)", content);
    }

    /**
     * Test INCR command with a preset key.
     * @throws Exception
     */
    @Test
    public void t08_incrWithExistingKeyTest() throws Exception {

        String uri = "/key1";

        mvc.perform(MockMvcRequestBuilders.put(uri)
                .contentType(MediaType.TEXT_PLAIN)
                .content("123"));

        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.patch(uri)).andReturn();

        int status = mvcResult.getResponse().getStatus();
        assertEquals(200, status);
        String content = mvcResult.getResponse().getContentAsString();
        assertEquals("(integer) 124", content);
    }

    /**
     * Test INCR command with a non-preset key.
     * @throws Exception
     */
    @Test
    public void t09_incrWithInexistingKeyTest() throws Exception {

        String uri = "/newKey";

        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.patch(uri)).andReturn();

        int status = mvcResult.getResponse().getStatus();
        assertEquals(200, status);
        String content = mvcResult.getResponse().getContentAsString();
        assertEquals("(integer) 0", content);
    }

    /**
     * Test INCR command with a preset key holding not numeric value.
     * @throws Exception
     */
    @Test
    public void t10_incrWithExistingKeyTest() throws Exception {

        String uri = "/key2";

        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.patch(uri)).andReturn();

        int status = mvcResult.getResponse().getStatus();
        assertEquals(200, status);
        String content = mvcResult.getResponse().getContentAsString();
        assertEquals("For input string: \"value2\"", content);
    }

    /**
     * Test ZADD command.
     * @throws Exception
     */
    @Test
    public void t11_zAdd() throws Exception {

        String uri = "/keyX";

        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post(uri)
                .contentType(MediaType.TEXT_PLAIN)
                .content("a=1")).andReturn();

        int status = mvcResult.getResponse().getStatus();
        assertEquals(200, status);
        String content = mvcResult.getResponse().getContentAsString();
        assertEquals("(integer) 1", content);
    }

    /**
     * Test ZCARD command.
     * @throws Exception
     */
    @Test
    public void t12_zCard() throws Exception {

        String uri = "/zcard/keyX";

        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get(uri)).andReturn();

        int status = mvcResult.getResponse().getStatus();
        assertEquals(200, status);
        String content = mvcResult.getResponse().getContentAsString();
        assertEquals("1", content);
    }

    /**
     * Test ZRANK command.
     * @throws Exception
     */
    @Test
    public void t13_zRank() throws Exception {

        String uri = "/keyX";
        String uriRank = "/keyX/3";

        mvc.perform(MockMvcRequestBuilders.post(uri)
                .contentType(MediaType.TEXT_PLAIN)
                .content("a=1"));

        mvc.perform(MockMvcRequestBuilders.post(uri)
                .contentType(MediaType.TEXT_PLAIN)
                .content("b=2"));

        mvc.perform(MockMvcRequestBuilders.post(uri)
                .contentType(MediaType.TEXT_PLAIN)
                .content("c=3"));

        mvc.perform(MockMvcRequestBuilders.post(uri)
                .contentType(MediaType.TEXT_PLAIN)
                .content("d=4"));

        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get(uriRank)).andReturn();

        int status = mvcResult.getResponse().getStatus();
        assertEquals(200, status);
        String content = mvcResult.getResponse().getContentAsString();
        assertEquals("2", content);
    }

    /**
     * Test ZRANK command.
     * @throws Exception
     */
    @Test
    public void t14_zRange() throws Exception {

        String uri = "/zrange/keyX?start=1&stop=3";

        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get(uri)).andReturn();

        int status = mvcResult.getResponse().getStatus();
        assertEquals(200, status);
        String content = mvcResult.getResponse().getContentAsString();
        assertEquals("1) \"2\"\n" +
                "2) \"3\"\n" +
                "3) \"4\"", content);
    }
}
