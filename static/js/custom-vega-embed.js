function customVegaEmbed( target, url, opts ) {
    function viewSource(source) {
        const header = `<html><head></head><body><pre><code class="json">`;
        const footer = `</code></pre></body></html>`;
        const win = window.open('');
        win.document.write(header + source + footer);
        win.document.title = `JSON Source`;
    }

    function post(url, data) {
        const editor = window.open(url);
        const wait = 10000;
        const step = 250;
        const {origin} = new URL(url);

        let count = Math.floor(wait / step);
      
        function listen(evt) {
          if (evt.source === editor) {
            count = 0;
            window.removeEventListener('message', listen, false);
          }
        }
        window.addEventListener('message', listen, false);
      
        function send() {
          if (count <= 0) {
            return;
          }
          editor.postMessage(data, origin);
          setTimeout(send, step);
          count -= 1;
        }
        setTimeout(send, step);
    }

    function addActions(view,workspace,data,spec) {
        const SVG_CIRCLES = `
        <svg viewBox="0 0 16 16" fill="currentColor" stroke="none" stroke-width="1" stroke-linecap="round" stroke-linejoin="round">
          <circle r="2" cy="8" cx="2"></circle>
          <circle r="2" cy="8" cx="8"></circle>
          <circle r="2" cy="8" cx="14"></circle>
        </svg>`;

        const details = $("<details>");
        workspace.append(details);
        const summary = $("<summary>").html(SVG_CIRCLES);
        details.append(summary);

        $(document).click(function(e){
            if ($(details).has(e.target).length==0) $(details).removeAttr('open');
        })
      
        var ctrl = $("<div>").addClass("vega-actions");
        details.append(ctrl);
        for (const ext of ['svg', 'png']) {
            const exportLink = document.createElement('a');
            exportLink.text = "Save as " + ext.toUpperCase();
            exportLink.href = '#';
            exportLink.target = '_blank';
            exportLink.download = 'visualization.' + ext;
            exportLink.addEventListener('mousedown', async function(e){
                e.preventDefault();
                this.href = await view.toImageURL(ext);
            });
            ctrl.append(exportLink);
        }

        const viewSourceLink = document.createElement('a');
        viewSourceLink.text = 'View Source';
        viewSourceLink.href = '#';
        viewSourceLink.addEventListener('click', function(e) {
            viewSource(JSON.stringify(data,null,2));
            e.preventDefault();
        });
        ctrl.append(viewSourceLink);

        const viewCompiledLink = document.createElement('a');
        viewCompiledLink.text = 'View Compiled Vega';
        viewCompiledLink.href = '#';
        viewCompiledLink.addEventListener('click', function(e) {
            viewSource(JSON.stringify(spec,null,2));
            e.preventDefault();
        });
        ctrl.append(viewCompiledLink);

        const editorLink = document.createElement('a');
        editorLink.text = 'Open in Vega Editor';
        editorLink.href = '#';
        editorLink.addEventListener('click', function (e) {
            post('https://vega.github.io/editor/', {
                mode: 'vega-lite',
                spec: JSON.stringify(data,null,2),
            });
            e.preventDefault();
        });
        ctrl.append(editorLink);
    }

    var workspace = $("<div>").addClass(["vega-embed","has-actions"]);

    var test_abort = function(){
        if (opts.abort && opts.abort()) reject(Error("Aborted"));
    }

    return new Promise(function(resolve,reject){
        $.ajax({
            url: url,
            dataType: 'json'
        }).done(function(data){
            try {
                test_abort();
                var spec = vegaLite.compile(data, {}).spec;
                test_abort();
                view = new vega.View(vega.parse(spec),{
                    renderer: 'canvas',
                    container: workspace.get(0)
                });
                test_abort();
                view.runAsync().then(function(){
                    addActions(view,workspace,data,spec);
                    test_abort();
                    $(target).empty().append(workspace);
                    resolve(view)
                });
            } catch (error) {
                reject(error);
            }
        }).fail(function(jqXHR, textStatus){
            reject(Error("Request failed: " + textStatus))
        })
    });
}