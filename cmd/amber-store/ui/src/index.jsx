import { render } from 'solid-js/web';

// Brand type: Montserrat (display/UI), Inter (body), JetBrains Mono (meta).
// Self-hosted so the admin console needs no internet.
import '@fontsource/montserrat/600.css';
import '@fontsource/montserrat/700.css';
import '@fontsource/montserrat/800.css';
import '@fontsource/montserrat/900.css';
import '@fontsource/inter/400.css';
import '@fontsource/inter/500.css';
import '@fontsource/inter/600.css';
import '@fontsource/jetbrains-mono/400.css';
import '@fontsource/jetbrains-mono/500.css';

import './app.css';
import App from './App';

render(() => <App />, document.getElementById('root'));
