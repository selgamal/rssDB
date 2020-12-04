if (!window.dash_clientside) {
  window.dash_clientside = {};
}
window.dash_clientside.clientside = {
  resize: function(value) {
    console.log("resizing..."); // for testing
    setTimeout(function() {
      window.dispatchEvent(new Event("resize"));
      console.log("fired resize");
    }, 500);
    return null;
  }
};

document.addEventListener("DOMContentLoaded", function (event) {
  var target = document.querySelector("head > title");
  var observer = new window.WebKitMutationObserver(function (mutations) {
    mutations.forEach(function (mutation) {
      if (mutation.target.textContent == "Updating...") {
        document.getElementById("refresh-button").disabled = true;
      } else {
        document.getElementById("refresh-button").disabled = false;
      }
    });
  });
  observer.observe(target, {
    subtree: true,
    characterData: true,
    childList: true,
  });

  var css =
    "body{margin: 0% 5% 5% 5%;} #refresh-button{width:100%} #refresh-button:hover{background: rgb(236, 236, 236);} #refresh-button:active{background: darkgray;color:white;} #refresh-button:disabled {opacity: .5;background: whitesmoke;cursor:default;}";
  var style = document.createElement("style");

  if (style.styleSheet) {
    style.styleSheet.cssText = css;
  } else {
    style.appendChild(document.createTextNode(css));
  }

  document.getElementsByTagName("head")[0].appendChild(style);

  
});


var checkExist = setInterval(function() {
  if (document.getElementById('date-picker-range')) {
    //  console.log("Exists!");
      document.getElementsByClassName('DateRangePickerInput__withBorder_2')[0].classList.remove('DateRangePickerInput__withBorder_2');
      document.getElementsByClassName('DateRangePickerInput__withBorder')[0].classList.remove('DateRangePickerInput__withBorder');

     clearInterval(checkExist);
  }
}, 100);

