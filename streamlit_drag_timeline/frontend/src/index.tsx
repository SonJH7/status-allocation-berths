import React from "react";
import ReactDOM from "react-dom/client";
import {
  Streamlit,
  withStreamlitConnection,
  type ComponentProps,
} from "streamlit-component-lib";

import Timeline from "./Timeline";
import "./styles.css";

const StreamlitTimeline = withStreamlitConnection((props: ComponentProps) => {
  const items = (props.args?.items as any[]) || [];
  const sentReadyRef = React.useRef(false);

  React.useEffect(() => {
    if (sentReadyRef.current) return;
    sentReadyRef.current = true;
    const id = window.setTimeout(() => {
      Streamlit.setComponentValue({ kind: "ready", events: [] });
    }, 0);
    return () => window.clearTimeout(id);
  }, []);

  return (
    <Timeline
      items={items}
      disabled={props.disabled}
      theme={props.theme}
      frameWidth={props.width}
      onEvents={(events) => Streamlit.setComponentValue(events)}
    />
  );
});

const root = ReactDOM.createRoot(document.getElementById("root") as HTMLElement);
root.render(<StreamlitTimeline />);

Streamlit.setComponentReady();
Streamlit.setFrameHeight(960);
