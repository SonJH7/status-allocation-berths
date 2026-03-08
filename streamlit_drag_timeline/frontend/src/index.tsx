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

  return (
    <Timeline
      items={items}
      disabled={props.disabled}
      theme={props.theme}
      frameWidth={props.width}
      onEvents={(payload) => Streamlit.setComponentValue(payload)}
    />
  );
});

const root = ReactDOM.createRoot(document.getElementById("root") as HTMLElement);
root.render(<StreamlitTimeline />);

Streamlit.setComponentReady();
Streamlit.setFrameHeight(960);
