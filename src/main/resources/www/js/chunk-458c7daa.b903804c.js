(window["webpackJsonp"]=window["webpackJsonp"]||[]).push([["chunk-458c7daa"],{1436:function(e,t,n){"use strict";n.r(t);var a=function(){var e=this,t=e.$createElement,n=e._self._c||t;return n("div",[n("el-form",{staticClass:"taskForm",attrs:{inline:"",model:e.search}},[n("el-form-item",{attrs:{label:"",prop:"instance"}},[n("el-input",{staticStyle:{width:"300px"},attrs:{placeholder:"请输入实例名称",clearable:"",maxlength:32},on:{clear:e.handleSearch},nativeOn:{keyup:function(t){return!t.type.indexOf("key")&&e._k(t.keyCode,"enter",13,t.key,"Enter")?null:e.handleSearch.apply(null,arguments)}},model:{value:e.search.Instance,callback:function(t){e.$set(e.search,"Instance",t)},expression:"search.Instance"}})],1),n("el-button",{attrs:{type:"primary"},on:{click:e.handleSearch}},[e._v("搜索")])],1),n("el-table",{attrs:{stripe:"","header-cell-style":{background:"#ddd",color:"#333"},data:e.tableData.slice((e.page.index-1)*e.page.size,e.page.size*e.page.index),border:""}},[n("el-table-column",{attrs:{prop:"Instance",label:"Instance"}}),n("el-table-column",{attrs:{prop:"SearchFrom",label:"SearchFrom"}}),n("el-table-column",{attrs:{prop:"FullCron",label:"FullCron"}}),n("el-table-column",{attrs:{prop:"Alias",label:"Alias"}}),n("el-table-column",{attrs:{prop:"DeltaCron",label:"DeltaCron"}}),n("el-table-column",{attrs:{prop:"IsVirtualPipe",label:"IsVirtualPipe"},scopedSlots:e._u([{key:"default",fn:function(t){var a=t.row;return[n("div",[e._v(e._s(a.IsVirtualPipe))])]}}])}),n("el-table-column",{attrs:{prop:"InstanceType",width:"160px",label:"InstanceType"}}),n("el-table-column",{attrs:{prop:"ReadFrom",label:"ReadFrom"}}),n("el-table-column",{attrs:{prop:"WriteTo",label:"WriteTo"}}),n("el-table-column",{attrs:{prop:"OpenTrans",label:"OpenTrans"},scopedSlots:e._u([{key:"default",fn:function(t){var a=t.row;return[n("div",[e._v(e._s(a.OpenTrans))])]}}])}),n("el-table-column",{attrs:{prop:"",label:""},scopedSlots:e._u([{key:"default",fn:function(t){var a=t.row;return[n("div",{staticClass:"flex flex-around"},[n("el-popover",{attrs:{trigger:"click"}},[n("el-button",{attrs:{slot:"reference",type:"text"},slot:"reference"},[e._v("信息")]),n("ul",[n("el-dropdown-item",{nativeOn:{click:function(t){return e.handleStatus(a)}}},[e._v("任务状态")]),n("el-dropdown-item",{nativeOn:{click:function(t){return e.handleDetail(a)}}},[e._v("任务信息")]),n("el-dropdown-item",{nativeOn:{click:function(t){return e.handleConfig(a)}}},[e._v("任务配置")])],1)],1),n("el-popover",{attrs:{trigger:"click"}},[n("el-button",{attrs:{slot:"reference",type:"text"},slot:"reference"},[e._v("管理")]),n("ul",[n("el-dropdown-item",{nativeOn:{click:function(t){return e.handleStop(a)}}},[e._v("停止增量任务")]),n("el-dropdown-item",{nativeOn:{click:function(t){return e.handleResume(a)}}},[e._v("恢复增量任务")]),n("el-dropdown-item",{nativeOn:{click:function(t){return e.handleRun(a)}}},[e._v("立即运行增量任务")]),n("el-dropdown-item",{nativeOn:{click:function(t){return e.handleStopFull(a)}}},[e._v("停止全量任务")]),n("el-dropdown-item",{nativeOn:{click:function(t){return e.handleResumeFull(a)}}},[e._v("恢复全量任务")]),n("el-dropdown-item",{nativeOn:{click:function(t){return e.handleRunFull(a)}}},[e._v("立即运行全量任务")]),n("el-dropdown-item",{nativeOn:{click:function(t){return e.handleDelete(a)}}},[e._v("删除任务")]),n("el-dropdown-item",{nativeOn:{click:function(t){return e.handleReset(a)}}},[e._v("重置断路器")])],1)],1)],1)]}}])})],1),n("div",{staticClass:"page"},[n("el-pagination",{attrs:{"current-page":e.page.index,"page-size":e.page.size,layout:"total, sizes, prev, pager, next, jumper",total:e.page.total,background:""},on:{"size-change":e.handlePageSize,"current-change":e.handlePageIndex}})],1),n("el-dialog",{attrs:{title:e.dialogTitle,visible:e.visible,"close-on-click-modal":!1},on:{"update:visible":function(t){e.visible=t}}},[n("code",[n("pre",{domProps:{innerHTML:e._s(e.detail)}})])]),n("el-dialog",{attrs:{title:"任务配置",visible:e.showXML,"close-on-click-modal":!1},on:{"update:visible":function(t){e.showXML=t}}},[n("codemirror",{attrs:{options:e.cmOptions},model:{value:e.code,callback:function(t){e.code=t},expression:"code"}})],1)],1)},i=[],l=(n("8dee"),n("b3d7"),n("1bc7"),n("d91d"),n("9b0e")),o={data:function(){return{code:"",showXML:!1,tableData:[],cmOptions:{tabSize:4,mode:"text/xml",theme:"ayu-dark",lineNumbers:!0,line:!0,readOnly:!0,matchBrackets:!0},originData:[],search:{Instance:""},visible:!1,page:{index:1,size:10,total:0},detail:"",dialogTitle:""}},methods:{handlePageSize:function(e){this.page.index=1,this.page.size=e},handlePageIndex:function(e){this.page.index=e},handleReset:function(e){var t=this;this.$confirm("是否重置该任务？","提示",{type:"warning"}).then((function(){l["a"].efm_doaction({ac:"resetBreaker",instance:e.Instance}).then((function(e){t.$notify({title:"成功",message:"重置成功",duration:2e3})}))}))},handleStatus:function(e){var t=this;this.dialogTitle="任务状态",l["a"].efm_doaction({ac:"getInstanceInfo",instance:e.Instance}).then((function(e){var n=e.response.datas,a={task:n.task};t.detail=t.syntaxHighlight(a),t.visible=!0}))},handleDetail:function(e){var t=this;this.dialogTitle="任务信息",l["a"].efm_doaction({ac:"getInstanceInfo",instance:e.Instance}).then((function(e){var n=e.response.datas,a={computer:n.computer,reader:n.reader,writer:n.writer};t.detail=t.syntaxHighlight(a),t.visible=!0}))},handleDelete:function(e){var t=this;this.$confirm("是否删除该任务？","提示",{type:"warning"}).then((function(){l["a"].efm_doaction({ac:"removeInstance",instance:e.Instance}).then((function(e){t.$notify({title:"成功",message:"删除成功",duration:2e3})}))}))},handleRun:function(e){var t=this;l["a"].efm_doaction({ac:"runNow",instance:e.Instance,jobtype:"increment"}).then((function(e){t.$notify({title:"成功",message:"运行增量任务成功",type:"success",duration:2e3})}))},handleRunFull:function(e){var t=this;l["a"].efm_doaction({ac:"runNow",instance:e.Instance,jobtype:"full"}).then((function(e){t.$notify({title:"成功",message:"运行全量任务成功",type:"success",duration:2e3})}))},handleResume:function(e){var t=this;l["a"].efm_doaction({ac:"resumeInstance",instance:e.Instance,type:"increment"}).then((function(e){t.$notify({title:"成功",message:"恢复增量任务成功",type:"success",duration:2e3})}))},handleResumeFull:function(e){var t=this;l["a"].efm_doaction({ac:"resumeInstance",instance:e.Instance,type:"full"}).then((function(e){t.$notify({title:"成功",message:"恢复全量任务成功",type:"success",duration:2e3})}))},handleStop:function(e){var t=this;l["a"].efm_doaction({ac:"stopInstance",instance:e.Instance,type:"increment"}).then((function(e){t.$notify({title:"成功",message:"停止增量任务成功",type:"success",duration:2e3})}))},handleStopFull:function(e){var t=this;l["a"].efm_doaction({ac:"stopInstance",instance:e.Instance,type:"full"}).then((function(e){t.$notify({title:"成功",message:"停止全量任务成功",type:"success",duration:2e3})}))},handleSearch:function(){var e=this,t=[];this.originData.filter((function(n){for(var a in e.search)-1!==n[a].indexOf(e.search[a])&&t.push(n)})),this.tableData=t,this.page.total=t.length},handleConfig:function(e){var t=this;l["a"].efm_doaction({ac:"getInstanceXml",instance:e.Instance}).then((function(e){var n=e.response.datas;t.code="string"===typeof n?n:"",t.showXML=!0}))},getTaskList:function(){var e=this;l["a"].efm_doaction({ac:"getinstances"}).then((function(t){var n=[];Object.values(t.response.datas).forEach((function(e){n=n.concat(e)})),e.page.total=n.length,e.tableData=Object.assign([],n),e.originData=Object.assign([],n)}))},syntaxHighlight:function(e){return"string"!==typeof e&&(e=JSON.stringify(e,void 0,2)),e=e.replace(/&/g,"&").replace(/</g,"<").replace(/>/g,">"),e.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g,(function(e){var t="number";return/^"/.test(e)?t=/:$/.test(e)?"key":"string":/true|false/.test(e)?t="boolean":/null/.test(e)&&(t="null"),'<span class="'+t+'">'+e+"</span>"}))}},created:function(){this.getTaskList()}},r=o,s=(n("b1ab"),n("cba8")),c=Object(s["a"])(r,a,i,!1,null,"4fd10dd0",null);t["default"]=c.exports},"1bc7":function(e,t,n){for(var a=n("25ba"),i=n("93ca"),l=n("84e8"),o=n("0b34"),r=n("065d"),s=n("953d"),c=n("839a"),u=c("iterator"),d=c("toStringTag"),f=s.Array,p={CSSRuleList:!0,CSSStyleDeclaration:!1,CSSValueList:!1,ClientRectList:!1,DOMRectList:!1,DOMStringList:!1,DOMTokenList:!0,DataTransferItemList:!1,FileList:!1,HTMLAllCollection:!1,HTMLCollection:!1,HTMLFormElement:!1,HTMLSelectElement:!1,MediaList:!0,MimeTypeArray:!1,NamedNodeMap:!1,NodeList:!0,PaintRequestList:!1,Plugin:!1,PluginArray:!1,SVGLengthList:!1,SVGNumberList:!1,SVGPathSegList:!1,SVGPointList:!1,SVGStringList:!1,SVGTransformList:!1,SourceBufferList:!1,StyleSheetList:!0,TextTrackCueList:!1,TextTrackList:!1,TouchList:!1},h=i(p),m=0;m<h.length;m++){var g,v=h[m],b=p[v],y=o[v],S=y&&y.prototype;if(S&&(S[u]||r(S,u,f),S[d]||r(S,d,v),s[v]=f,b))for(g in a)S[g]||l(S,g,a[g],!0)}},"69b0":function(e,t){e.exports=Object.is||function(e,t){return e===t?0!==e||1/e===1/t:e!=e&&t!=t}},8387:function(e,t,n){},b1ab:function(e,t,n){"use strict";n("8387")},b3d7:function(e,t,n){var a=n("e99b"),i=n("d3ef")(!1);a(a.S,"Object",{values:function(e){return i(e)}})},d3ef:function(e,t,n){var a=n("26df"),i=n("93ca"),l=n("3471"),o=n("35d4").f;e.exports=function(e){return function(t){var n,r=l(t),s=i(r),c=s.length,u=0,d=[];while(c>u)n=s[u++],a&&!o.call(r,n)||d.push(e?[n,r[n]]:r[n]);return d}}},d91d:function(e,t,n){"use strict";var a=n("a86f"),i=n("69b0"),l=n("f417");n("c46f")("search",1,(function(e,t,n,o){return[function(n){var a=e(this),i=void 0==n?void 0:n[t];return void 0!==i?i.call(n,a):new RegExp(n)[t](String(a))},function(e){var t=o(n,e,this);if(t.done)return t.value;var r=a(e),s=String(this),c=r.lastIndex;i(c,0)||(r.lastIndex=0);var u=l(r,s);return i(r.lastIndex,c)||(r.lastIndex=c),null===u?-1:u.index}]}))}}]);