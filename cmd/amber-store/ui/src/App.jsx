import { Show, createResource, createSignal } from 'solid-js';
import * as api from './api';
import Login from './Login';
import Console from './Console';

// The app has two states: signed out (login hero) and the keys
// console. A 401 from any API call drops back to the login screen.
export default function App() {
  const [authed, setAuthed] = createSignal(false);
  const [checked] = createResource(async () => {
    try {
      await api.checkSession();
      setAuthed(true);
    } catch {
      setAuthed(false);
    }
    return true;
  });

  return (
    <Show when={checked()}>
      <Show
        when={authed()}
        fallback={<Login onLogin={() => setAuthed(true)} />}
      >
        <Console onSignOut={() => setAuthed(false)} />
      </Show>
    </Show>
  );
}
