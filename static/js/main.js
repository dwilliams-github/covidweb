function country() {
    $("#v0 .vega").addClass("shade");
    vegaEmbed( "#v0 .vega", "/api/country/graph?"+$("#selcountry").serialize() )
    .then($("#v0 .vega").removeClass("shade"))
    .catch(console.error);
}
function state() {
    $("#v10 .vega").addClass("shade");
    vegaEmbed( "#v10 .vega", "/api/state/graph?"+$("#selstate").serialize() + "&" + $("#modestate").serialize() )
    .then($("#v10 .vega").removeClass("shade"))
    .catch(console.error);
}
function stateComposite() {
    $("#v11 .vega").addClass("shade");
    vegaEmbed( "#v11 .vega", "/api/state/composite?"+$("#modecompstate").serialize() )
    .then($("#v11 .vega").removeClass("shade"))
    .catch(console.error);
}
function county() {
    $("#v20 .vega").addClass("shade");
    vegaEmbed( "#v20 .vega", "/api/county/simple?"+$("#selcounty").serialize() )
    .then($("#v20 .vega").removeClass("shade"))
    .catch(console.error);
}
function renderView(view) {
    switch(view) {
        case 0: return country();
        case 10: return state();
        case 11: return stateComposite();
        case 20: return county();
    }
}
function refresh() {
    $("div.menu .selected").each(function(){
        renderView( parseInt($(this).attr("id").substring(1)) );
    });
}
$(document).ready(function(){
    $("#selcountry,#selstate,#modestate,#modecompstate,#selcounty").select2();
    $("#selcountry").change(country);
    $("#selstate,#modestate").change(state);
    $("#modecompstate").change(stateComposite);
    $("#selcounty").change(county);
    $("div.menu .option").click(function(){
        $("div.menu .option").removeClass("selected");
        $(this).addClass("selected");
        $(".view").addClass("hidden");
        $("#v" + $(this).attr("id").substr(1)).removeClass('hidden');
        refresh();
    });
    
    refresh();
});