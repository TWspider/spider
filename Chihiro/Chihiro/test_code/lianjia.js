
define("mapV2/js/model", function(require, e) {
    function t(e) {}
    var n = (require("mapV2/js/mapView"),
    require("mapV2/js/util/md5"));
    return t.prototype = {
        bubbleAjax: null,
        ditieBubbleAjax: null,
        cardAjax: null,
        houseAjax: null,
        centerAjax: null,
        getAjaxUrl: function(e) {
            return {
                bubble: "map/search/ershoufang/",
                ditieBubble: "map/search/stationcommunity/",
                house: "map/resblock/ershoufanglist/",
                center: "map/bizcirclecore/",
                resblockCard: "map/resblock/ershoufangcard/",
                ditieLine: "map/subway/line/",
                singleDitieInfo: "map/subway/station/",
                stationInfo: "map/search/stationershoufang/",
                sug: "map/suggest/index"
            }[e]
        },
        getTransObj: function(e, t) {
            var n = {};
            switch (e) {
            case "bubble":
                n = {
                    city_id: window.g_conf.cityId,
                    group_type: t.group,
                    max_lat: t.max_latitude,
                    min_lat: t.min_latitude,
                    max_lng: t.max_longitude,
                    min_lng: t.min_longitude,
                    sug_id: t.sug_id || "",
                    sug_type: t.sug_type || "",
                    filters: JSON.stringify(window.g_conf.filter)
                };
                break;
            case "ditieBubble":
                n = {
                    city_id: window.g_conf.cityId,
                    line_id: t.line_id,
                    max_lat: t.max_latitude,
                    min_lat: t.min_latitude,
                    max_lng: t.max_longitude,
                    min_lng: t.min_longitude,
                    filters: JSON.stringify(window.g_conf.filter)
                },
                t.station_id && (n.station_id = t.station_id);
                break;
            case "house":
                n = {
                    id: t.ids,
                    order: t.order || 0,
                    page: t.limit_offset + 1,
                    filters: JSON.stringify(window.g_conf.filter)
                };
                break;
            case "center":
                n = {
                    city_id: window.g_conf.cityId,
                    id: t[0].id,
                    filter: null,
                    source: "ljapi"
                };
                break;
            case "resblockCard":
                n = {
                    id: t.ids
                };
                break;
            case "ditieLine":
                n = {
                    city_id: g_conf.cityId
                };
                break;
            case "singleDitieInfo":
                n = {
                    city_id: g_conf.cityId,
                    line_id: t.id
                };
                break;
            case "stationInfo":
                n = {
                    city_id: window.g_conf.cityId,
                    station_id: t.id,
                    filters: JSON.stringify(window.g_conf.filter)
                };
                break;
            case "sug":
                n = {
                    city_id: g_conf.cityId,
                    query: t
                };
                break;
            default:
                n = {}
            }
            return (n.filters && "rp0" == n.filters || "" == n.filters) && delete n.filters,
            n
        },
        getMd5: function(e) {
            var t = []
              , a = "";
            for (var i in e)
                t.push(i);
            t.sort();
            for (var i = 0; i < t.length; i++) {
                var o = t[i];
                "filters" !== o && (a += o + "=" + e[o])
            }
            return a ? (window.md5 = n,
            n("vfkpbin1ix2rb88gfjebs0f60cbvhedl" + a)) : ""
        },
        ajax: function(e, t, n, a) {
            if ("ditieLine" == e && !$(".toSubway").attr("data-subway"))
                return !1;
            var i, o = "//ajax.lianjia.com/";
            "test" == $.env.getEnv() ? o = "//testajax.lianjia.com/" : "dev" == $.env.getEnv() ? o = "//devajax.lianjia.com:8181/" : /preview\d?\-/.test(location.host) && (o = "//" + location.host.split("-")[0] + "-ajax.lianjia.com/");
            var r = this.getAjaxUrl(e)
              , l = this.getTransObj(e, t);
            l.request_ts = (new Date).getTime();
            var s = this.getMd5(l);
            l.source = "ljpc",
            l.authorization = s,
            r && (this[e + "Ajax"] = $.ajax({
                url: o + r,
                dataType: "jsonp",
                data: l,
                method: "GET",
                success: function(t) {
                    this[e + "Ajax"] = null,
                    i.cbFunc && i.cbFunc(t)
                },
                error: function() {}
            }),
            i = this[e + "Ajax"],
            i.cbFunc = n)
        },
        getBubbles: function(e, t, n) {
            this.ajax("bubble", e, t, n)
        },
        getDitieBubbles: function(e, t, n) {
            this.ajax("ditieBubble", e, t, n)
        },
        formatParams: function(e, t) {
            var n = {};
            return $.each(t, function(t, a) {
                n[a] = e[a]
            }),
            $.param(n)
        },
        getCards: function(e, t, n) {
            this.ajax("card", e, t, n)
        },
        getCenter: function(e, t) {
            this.ajax("center", e, t)
        },
        getHouseList: function(e, t, n) {
            this.ajax("house", e, t, n)
        },
        getResblockCard: function(e, t) {
            this.ajax("resblockCard", e, t, !1)
        },
        getDitieLine: function(e, t) {
            this.ajax("ditieLine", e, t, !1)
        },
        getSingleDitieLine: function(e, t) {
            this.ajax("singleDitieInfo", e, t, !1)
        },
        getStationInfo: function(e, t) {
            this.ajax("stationInfo", e, t, !1)
        },
        getSug: function(e, t) {
            this.ajax("sug", e, t)
        }
    },
    t
})