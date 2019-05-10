package com.aquiris.redis.controller;

import com.aquiris.redis.service.RedisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@RestController
public class RedisController {

    @Autowired
    private RedisService redisService;

    /**
     * Implementation of SET command.
     * @param myvalue value
     * @param mykey key
     * @return "OK" if successful.
     */
    @RequestMapping(method = RequestMethod.PUT, value = "/{mykey}")
    public String cmdSET(@RequestBody String myvalue, @PathVariable("mykey") String mykey) {
        redisService.put(mykey, myvalue);
        return "OK";
    }

    /**
     * Implementation of SET command with EX argument.
     * @param myvalue value
     * @param mykey key
     * @param expire expire
     * @return "OK" if successful.
     */
    @RequestMapping(method = RequestMethod.PUT, value = "/{mykey}/{expire}")
    public String cmdSETwithExpire(@RequestBody String myvalue, @PathVariable("mykey") String mykey, @PathVariable("expire") String expire) {
        redisService.put(mykey, myvalue, expire);
        return "OK";
    }

    /**
     * Implementation of GET command.
     * @param mykey key
     * @return The value of the key if exists or "(nil)" if doesn't.
     */
    @RequestMapping(method = RequestMethod.GET, value = "/{mykey}")
    public String cmdGET(@PathVariable("mykey") String mykey) {
        return redisService.get(mykey);
    }

    /**
     * Implementation of DEL command.
     * @param mykey key
     * @return "OK" if successful.
     */
    @RequestMapping(method = RequestMethod.DELETE, value = "/{mykey}")
    public String cmdDEL(@PathVariable("mykey") String mykey) {
        return redisService.delete(mykey);
    }

    /**
     * Implementation of DBSIZE command.
     * @return the number of keys.
     */
    @RequestMapping(method = RequestMethod.GET)
    public String cmdDBSIZE() {
        return String.valueOf(redisService.size());
    }

    /**
     * Implementation of INCR command.
     * @param mykey key
     * @return the value of the key increased by 1.
     */
    @RequestMapping(method = RequestMethod.PATCH, value = "/{mykey}")
    public String cmdINCR(@PathVariable("mykey") String mykey) {
        return redisService.incr(mykey);
    }

    /**
     * Implementation of ZADD command. Body content has to come with a pair of key/value separated by "=" symbol.
     * @param myvalue value
     * @param mykey key
     * @return "(integer) 1" if successful.
     */
    @RequestMapping(method = RequestMethod.POST, value = "/{mykey}")
    public String cmdZADD(@RequestBody String myvalue, @PathVariable("mykey") String mykey) {
        redisService.zAdd(mykey, myvalue);
        return "(integer) 1";
    }

    /**
     * Implementation of ZCARD command.
     * @param mykey key
     * @return The number of members of the keys.
     */
    @RequestMapping(method = RequestMethod.GET, value = "/zcard/{mykey}")
    public String cmdZCARD(@PathVariable("mykey") String mykey) {
        return String.valueOf(redisService.zSize(mykey));
    }

    /**
     * Implementation of ZRANK command.
     * @param mykey key
     * @param myValue value
     * @return The number of members of the keys.
     */
    @RequestMapping(method = RequestMethod.GET, value = "/{mykey}/{myValue}")
    public String cmdZRANK(@PathVariable("mykey") String mykey, @PathVariable("myValue") String myValue) {
        return String.valueOf(redisService.zRank(mykey, myValue));
    }

    /**
     * Implementation of ZRANGE command.
     * @param mykey key
     * @param start beginning of the range
     * @param stop end of the range
     * @return The number of members of the keys.
     */
    @RequestMapping(method = RequestMethod.GET, value = "zrange/{mykey}")
    public String cmdZRANGE(@PathVariable("mykey") String mykey, @RequestParam("start") int start, @RequestParam("stop") int stop) {
        return redisService.zRange(mykey, start, stop);
    }

    /**
     * Not required. Just for dev purposes.
     * @return
     */
    @RequestMapping(method = RequestMethod.GET,
            value = "/all"
            ,produces = MediaType.APPLICATION_JSON_VALUE)
    public String getAll() {
        return redisService.getAll();
    }

    /**
     * Not required. Just for dev purposes.
     * @return
     */
    @RequestMapping(method = RequestMethod.GET,
            value = "/zall",
            produces = MediaType.APPLICATION_JSON_VALUE)
    public String getZAll() {
        return redisService.getZAll();
    }
}
